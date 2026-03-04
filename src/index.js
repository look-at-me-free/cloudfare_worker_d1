export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    // Health check
    if (url.pathname === "/" || url.pathname === "/health") {
      return new Response("drive worker alive");
    }

    // POST /titles  (body: links or IDs, one per line)
    if (url.pathname === "/titles") {
      if (request.method !== "POST") {
        return new Response("Use POST. Body: links or IDs, one per line.", { status: 405 });
      }

      if (!env.GDRIVE_API_KEY) {
        return new Response("Missing env.GDRIVE_API_KEY", { status: 500 });
      }
      if (!env.DB) {
        return new Response("Missing D1 binding env.DB (add D1 binding named DB).", { status: 500 });
      }

      const body = await request.text();

      function extractId(line) {
        const s = line.trim();
        if (!s) return null;

        // raw id
        if (/^[a-zA-Z0-9_-]{20,}$/.test(s)) return s;

        // /file/d/<ID>/
        let m = s.match(/\/file\/d\/([a-zA-Z0-9_-]{20,})/);
        if (m) return m[1];

        // open?id=<ID>
        m = s.match(/[?&]id=([a-zA-Z0-9_-]{20,})/);
        if (m) return m[1];

        // uc?id=<ID>
        m = s.match(/\/uc\?id=([a-zA-Z0-9_-]{20,})/);
        if (m) return m[1];

        return null;
      }

      const ids = [];
      const seen = new Set();
      for (const line of body.split(/\r?\n/)) {
        const id = extractId(line);
        if (id && !seen.has(id)) {
          seen.add(id);
          ids.push(id);
        }
      }

      if (!ids.length) {
        return new Response("No Drive IDs found in POST body.", { status: 400 });
      }

      // Ensure table exists (safe to run every time)
      await env.DB.exec(`
        CREATE TABLE IF NOT EXISTS files (
          id TEXT PRIMARY KEY,
          title TEXT,
          updated_at TEXT
        );
      `);

      const CONCURRENCY = 12;

      async function getTitle(id) {
        const api = new URL(`https://www.googleapis.com/drive/v3/files/${encodeURIComponent(id)}`);
        api.searchParams.set("fields", "name");
        api.searchParams.set("key", env.GDRIVE_API_KEY);

        const r = await fetch(api.toString(), { headers: { "Accept": "application/json" } });
        if (!r.ok) return { id, title: `__ERROR_${r.status}__` };

        const j = await r.json();
        return { id, title: j?.name ?? "__NO_NAME__" };
      }

      const out = [];

      for (let i = 0; i < ids.length; i += CONCURRENCY) {
        const slice = ids.slice(i, i + CONCURRENCY);
        const res = await Promise.all(slice.map(getTitle));
        out.push(...res);

        // Batch upserts
        const stmts = res.map(({ id, title }) =>
          env.DB.prepare(
            `INSERT INTO files (id, title, updated_at)
             VALUES (?1, ?2, datetime('now'))
             ON CONFLICT(id) DO UPDATE SET
               title=excluded.title,
               updated_at=excluded.updated_at`
          ).bind(id, title)
        );
        await env.DB.batch(stmts);
      }

      // Return a downloadable mapping too
      const text = out.map(x => `${x.id} | ${x.title}`).join("\n") + "\n";

      return new Response(text, {
        headers: {
          "content-type": "text/plain; charset=utf-8",
          "content-disposition": `attachment; filename="drive_titles.txt"`
        }
      });
    }

    // GET /dump (see what’s stored)
    if (url.pathname === "/dump") {
      if (!env.DB) return new Response("Missing D1 binding env.DB", { status: 500 });
      const rs = await env.DB
        .prepare("SELECT id, title, updated_at FROM files ORDER BY updated_at DESC LIMIT 2000")
        .all();
      const lines =
        (rs.results || []).map(r => `${r.id} | ${r.title} | ${r.updated_at}`).join("\n") + "\n";
      return new Response(lines, { headers: { "content-type": "text/plain; charset=utf-8" } });
    }

    return new Response("Not found", { status: 404 });
  }
};
