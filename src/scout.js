// scout-queue consumer — finds the careers/jobs page URL for each business
import puppeteer from '@cloudflare/puppeteer';

const JOB_LINK_RE = /\b(jobs|careers|hiring|employment|work[-\s]with[-\s]us|join[-\s]us|openings|opportunities)\b/i;

export async function handleScoutBatch(batch, env) {
  for (const message of batch.messages) {
    const { business_id } = message.body;
    try {
      await scoutBusiness(business_id, env);
      message.ack();
    } catch (err) {
      console.error(`Scout error for business ${business_id}:`, err.message);
      await logCrawl(env, business_id, 'error', `scout: ${err.message}`);
      message.retry();
    }
  }
}

async function scoutBusiness(business_id, env) {
  const biz = await env.DB.prepare(
    'SELECT homepage_url FROM businesses WHERE id = ?'
  ).bind(business_id).first();

  if (!biz) return;

  let careers_url = biz.homepage_url; // fallback

  const browser = await puppeteer.launch(env.BROWSER);
  try {
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (compatible; SkagitJobsBot/1.0)');
    await page.goto(biz.homepage_url, { waitUntil: 'domcontentloaded', timeout: 20000 });

    const links = await page.evaluate(() =>
      Array.from(document.querySelectorAll('a[href]')).map(a => ({
        href: a.href,
        text: a.textContent.trim(),
      }))
    );

    const match = links.find(
      l => JOB_LINK_RE.test(l.text) || JOB_LINK_RE.test(l.href)
    );

    if (match?.href) careers_url = match.href;
  } finally {
    await browser.close();
  }

  await env.DB.prepare(
    'UPDATE businesses SET careers_url = ? WHERE id = ?'
  ).bind(careers_url, business_id).run();

  await logCrawl(env, business_id, 'scout_ok', careers_url);
}

async function logCrawl(env, business_id, status, message) {
  await env.DB.prepare(
    'INSERT INTO crawl_log (business_id, status, message) VALUES (?, ?, ?)'
  ).bind(business_id, status, message ?? null).run();
}
