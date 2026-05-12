// crawl-queue consumer — fetches careers pages, diffs against last hash, saves to R2
import puppeteer from '@cloudflare/puppeteer';

export async function handleCrawlBatch(batch, env) {
  for (const message of batch.messages) {
    const { business_id, careers_url } = message.body;
    try {
      await crawlBusiness(business_id, careers_url, env);
      message.ack();
    } catch (err) {
      console.error(`Crawl error for business ${business_id}:`, err.message);
      await handleFailure(business_id, err.message, env);
      message.ack(); // ack intentionally — cron retries tomorrow, not the queue
    }
  }
}

async function crawlBusiness(business_id, careers_url, env) {
  // Render page and extract text
  const browser = await puppeteer.launch(env.BROWSER);
  let pageText;
  try {
    const page = await browser.newPage();
    await page.setUserAgent('Mozilla/5.0 (compatible; SkagitJobsBot/1.0)');
    await page.goto(careers_url, { waitUntil: 'networkidle0', timeout: 30000 });
    pageText = await page.evaluate(() => document.body.innerText);
  } finally {
    await browser.close();
  }

  const newHash = await sha256(pageText);

  const biz = await env.DB.prepare(
    'SELECT last_hash FROM businesses WHERE id = ?'
  ).bind(business_id).first();

  // No change — just update timestamp and move on
  if (biz?.last_hash === newHash) {
    await env.DB.prepare(
      'UPDATE businesses SET last_crawled_at = unixepoch() WHERE id = ?'
    ).bind(business_id).run();
    await logCrawl(env, business_id, 'unchanged', null);
    return;
  }

  // Changed — save snapshot to R2
  const today = new Date().toISOString().split('T')[0];
  const r2Key = `careers/${business_id}/${today}.txt`;
  await env.R2.put(r2Key, pageText);

  // Update business record, clear failure count on successful crawl
  await env.DB.prepare(
    `UPDATE businesses
     SET last_hash = ?, last_crawled_at = unixepoch(), crawl_failure_count = 0
     WHERE id = ?`
  ).bind(newHash, business_id).run();

  await logCrawl(env, business_id, 'changed', r2Key);

  // Kick off AI extraction
  await env.EXTRACT_QUEUE.send({ business_id, r2_key: r2Key });
}

async function handleFailure(business_id, errorMsg, env) {
  // Increment failure count; deactivate after 5 consecutive failures
  await env.DB.prepare(
    `UPDATE businesses
     SET crawl_failure_count = crawl_failure_count + 1,
         is_active = CASE WHEN crawl_failure_count + 1 >= 5 THEN 0 ELSE is_active END
     WHERE id = ?`
  ).bind(business_id).run();

  await logCrawl(env, business_id, 'error', errorMsg);
}

async function sha256(str) {
  const buf = new TextEncoder().encode(str);
  const hash = await crypto.subtle.digest('SHA-256', buf);
  return Array.from(new Uint8Array(hash))
    .map(b => b.toString(16).padStart(2, '0'))
    .join('');
}

async function logCrawl(env, business_id, status, message) {
  await env.DB.prepare(
    'INSERT INTO crawl_log (business_id, status, message) VALUES (?, ?, ?)'
  ).bind(business_id, status, message ?? null).run();
}
