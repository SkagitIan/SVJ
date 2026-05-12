import { handleFetch } from './api.js';
import { handleCron } from './cron.js';
import { handleScoutBatch } from './scout.js';
import { handleCrawlBatch } from './crawl.js';
import { handleExtractBatch } from './extract.js';

export default {
  async fetch(request, env, ctx) {
    return handleFetch(request, env, ctx);
  },

  async scheduled(event, env, ctx) {
    ctx.waitUntil(handleCron(event, env));
  },

  async queue(batch, env, ctx) {
    switch (batch.queue) {
      case 'scout-queue':
        return handleScoutBatch(batch, env);
      case 'crawl-queue':
        return handleCrawlBatch(batch, env);
      case 'extract-queue':
        return handleExtractBatch(batch, env);
      default:
        console.error(`Unknown queue: ${batch.queue}`);
    }
  },
};
