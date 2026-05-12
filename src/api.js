// HTTP handler — serves the public job API and the protected admin dashboard API

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
  'Access-Control-Allow-Headers': 'Content-Type, Authorization',
};

export async function handleFetch(request, env) {
  if (request.method === 'OPTIONS') {
    return new Response(null, { status: 204, headers: CORS });
  }

  const url = new URL(request.url);
  const path = url.pathname;
  const auth = request.headers.get('Authorization') ?? '';

  try {
    if (path === '/api/jobs')         return ok(await getJobs(url, env));
    if (path.match(/^\/api\/jobs\/\d+$/)) return ok(await getJob(path, env));
    if (path === '/api/stats')        return ok(await getStats(env));

    if (path === '/api/dashboard') {
      if (auth !== `Bearer ${env.ADMIN_SECRET}`) return err('Unauthorized', 401);
      return ok(await getDashboard(url, env));
    }

    if (path === '/api/import') {
      if (auth !== `Bearer ${env.IMPORT_SECRET}`) return err('Unauthorized', 401);
      if (request.method !== 'POST') return err('POST only', 405);
      return ok(await handleImport(request, env));
    }

    return err('Not found', 404);
  } catch (e) {
    console.error(e);
    return err('Internal server error', 500);
  }
}

// ── Public: Job Listings ──────────────────────────────────────────────────────

async function getJobs(url, env) {
  const city     = url.searchParams.get('city')     ?? '';
  const industry = url.searchParams.get('industry') ?? '';
  const q        = url.searchParams.get('q')        ?? '';
  const page     = Math.max(1, parseInt(url.searchParams.get('page') ?? '1'));
  const limit    = 20;
  const offset   = (page - 1) * limit;

  let sql = `
    SELECT jp.id, jp.job_title, jp.department, jp.salary_info, jp.application_url,
           jp.first_seen_at, jp.last_seen_at,
           b.business_name, b.city, b.industry
    FROM job_postings jp
    JOIN businesses b ON jp.business_id = b.id
    WHERE jp.is_active = 1
  `;
  const params = [];

  if (city)     { sql += ' AND b.city = ?';               params.push(city); }
  if (industry) { sql += ' AND b.industry = ?';           params.push(industry); }
  if (q)        { sql += ' AND jp.job_title LIKE ?';      params.push(`%${q}%`); }

  sql += ' ORDER BY jp.first_seen_at DESC LIMIT ? OFFSET ?';
  params.push(limit, offset);

  const { results } = await env.DB.prepare(sql).bind(...params).all();
  return { jobs: results, page, limit };
}

async function getJob(path, env) {
  const id = parseInt(path.split('/').pop());
  const row = await env.DB.prepare(
    `SELECT jp.*, b.business_name, b.city, b.industry, b.homepage_url
     FROM job_postings jp JOIN businesses b ON jp.business_id = b.id
     WHERE jp.id = ? AND jp.is_active = 1`
  ).bind(id).first();
  if (!row) throw Object.assign(new Error('Not found'), { status: 404 });
  return row;
}

// ── Public: Stats ─────────────────────────────────────────────────────────────

async function getStats(env) {
  const row = await env.DB.prepare(`
    SELECT
      (SELECT COUNT(*) FROM businesses  WHERE is_active = 1)  AS active_businesses,
      (SELECT COUNT(*) FROM job_postings WHERE is_active = 1) AS total_jobs,
      (SELECT COUNT(*) FROM job_postings WHERE first_seen_at > unixepoch() - 86400) AS jobs_today,
      (SELECT COUNT(*) FROM crawl_log   WHERE status = 'changed' AND created_at > unixepoch() - 86400) AS changed_today,
      (SELECT COUNT(*) FROM crawl_log   WHERE status = 'error'   AND created_at > unixepoch() - 86400) AS errors_today,
      (SELECT MAX(created_at) FROM crawl_log) AS last_activity
  `).first();
  return row;
}

// ── Admin: Dashboard ──────────────────────────────────────────────────────────

async function getDashboard(url, env) {
  const logPage   = Math.max(1, parseInt(url.searchParams.get('log_page') ?? '1'));
  const logStatus = url.searchParams.get('status') ?? '';
  const logLimit  = 50;
  const logOffset = (logPage - 1) * logLimit;

  // Summary stats
  const stats = await getStats(env);

  // Jobs by day (all time)
  const { results: jobsByDay } = await env.DB.prepare(`
    SELECT date(first_seen_at, 'unixepoch') AS day, COUNT(*) AS count
    FROM job_postings
    GROUP BY day
    ORDER BY day ASC
  `).all();

  // Crawl results by day — last 30 days
  const { results: crawlsByDay } = await env.DB.prepare(`
    SELECT date(created_at, 'unixepoch') AS day, status, COUNT(*) AS count
    FROM crawl_log
    WHERE created_at > unixepoch() - (30 * 86400)
    GROUP BY day, status
    ORDER BY day ASC
  `).all();

  // Recent crawl log (with business name)
  let logSql = `
    SELECT cl.id, cl.status, cl.message, cl.created_at,
           b.business_name, b.city
    FROM crawl_log cl
    LEFT JOIN businesses b ON cl.business_id = b.id
  `;
  const logParams = [];
  if (logStatus) { logSql += ' WHERE cl.status = ?'; logParams.push(logStatus); }
  logSql += ' ORDER BY cl.created_at DESC LIMIT ? OFFSET ?';
  logParams.push(logLimit, logOffset);
  const { results: recentLog } = await env.DB.prepare(logSql).bind(...logParams).all();

  // Log total count (for pagination)
  let countSql = 'SELECT COUNT(*) AS total FROM crawl_log';
  const countParams = [];
  if (logStatus) { countSql += ' WHERE status = ?'; countParams.push(logStatus); }
  const logCount = await env.DB.prepare(countSql).bind(...countParams).first();

  // Failing businesses
  const { results: failing } = await env.DB.prepare(`
    SELECT id, business_name, city, industry, homepage_url,
           careers_url, crawl_failure_count, is_active, last_crawled_at,
           (SELECT message FROM crawl_log WHERE business_id = businesses.id
            AND status = 'error' ORDER BY created_at DESC LIMIT 1) AS last_error
    FROM businesses
    WHERE crawl_failure_count > 0
    ORDER BY crawl_failure_count DESC
  `).all();

  // Jobs by industry
  const { results: byIndustry } = await env.DB.prepare(`
    SELECT b.industry, COUNT(*) AS count
    FROM job_postings jp JOIN businesses b ON jp.business_id = b.id
    WHERE jp.is_active = 1
    GROUP BY b.industry
    ORDER BY count DESC
  `).all();

  return {
    stats,
    jobsByDay,
    crawlsByDay,
    recentLog,
    logTotal: logCount?.total ?? 0,
    logPage,
    logLimit,
    failing: failing.results ?? failing,
    byIndustry,
  };
}

// ── Admin: Import ─────────────────────────────────────────────────────────────

async function handleImport(request, env) {
  const body = await request.json();
  const records = Array.isArray(body) ? body : [body];

  let imported = 0;
  let skipped  = 0;
  const newIds = [];

  for (const r of records) {
    const name    = r.business_name || r.name;
    const city    = normalizeCity(r.city || r.full_address || '');
    const industry = normalizeIndustry(r.industry || r.type || '');
    const url     = r.homepage_url || r.site || r.website;

    if (!name || !url) { skipped++; continue; }

    const existing = await env.DB.prepare(
      'SELECT id FROM businesses WHERE homepage_url = ?'
    ).bind(url).first();

    if (existing) { skipped++; continue; }

    const result = await env.DB.prepare(
      `INSERT INTO businesses (business_name, city, industry, homepage_url)
       VALUES (?, ?, ?, ?)`
    ).bind(name, city, industry, url).run();

    if (result.meta?.last_row_id) newIds.push(result.meta.last_row_id);
    imported++;
  }

  // Enqueue new businesses for scouting
  if (newIds.length) {
    await env.SCOUT_QUEUE.sendBatch(newIds.map(id => ({ body: { business_id: id } })));
  }

  return { imported, skipped, scouting: newIds.length };
}

// ── Helpers ───────────────────────────────────────────────────────────────────

function normalizeCity(raw) {
  const CITIES = [
    'Mount Vernon', 'Burlington', 'Sedro-Woolley', 'Anacortes',
    'Concrete', 'La Conner', 'Bow', 'Bellingham',
  ];
  const found = CITIES.find(c => raw.toLowerCase().includes(c.toLowerCase()));
  return found ?? raw.split(',')[0].trim();
}

function normalizeIndustry(raw) {
  const MAP = {
    manufacturing: 'Manufacturing',
    agriculture: 'Agriculture', farm: 'Agriculture',
    health: 'Healthcare', medical: 'Healthcare', hospital: 'Healthcare',
    restaurant: 'Hospitality', brewery: 'Hospitality', hotel: 'Hospitality',
    construction: 'Construction',
    technology: 'Technology', software: 'Technology',
    retail: 'Retail', store: 'Retail',
  };
  const lower = raw.toLowerCase();
  for (const [key, val] of Object.entries(MAP)) {
    if (lower.includes(key)) return val;
  }
  return 'Other';
}

function ok(data) {
  return new Response(JSON.stringify(data), {
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}

function err(msg, status = 400) {
  return new Response(JSON.stringify({ error: msg }), {
    status,
    headers: { 'Content-Type': 'application/json', ...CORS },
  });
}
