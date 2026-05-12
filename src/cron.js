// Runs daily at 3 AM — fans out all active businesses into crawl-queue

export async function handleCron(event, env) {
  const businesses = await env.DB.prepare(
    `SELECT id, careers_url FROM businesses
     WHERE is_active = 1 AND careers_url IS NOT NULL`
  ).all();

  if (!businesses.results.length) {
    console.log('Cron: no active businesses to crawl');
    return;
  }

  // Batch-send to crawl queue
  const messages = businesses.results.map(b => ({
    body: { business_id: b.id, careers_url: b.careers_url },
  }));

  // sendBatch supports up to 100 messages per call
  for (let i = 0; i < messages.length; i += 100) {
    await env.CRAWL_QUEUE.sendBatch(messages.slice(i, i + 100));
  }

  console.log(`Cron: enqueued ${businesses.results.length} businesses for crawl`);
}
