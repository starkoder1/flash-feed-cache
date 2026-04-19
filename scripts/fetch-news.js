#!/usr/bin/env node

/**
 * NewsData.io News Fetcher
 *
 * Fetches news articles from NewsData.io across 15 category feeds
 * (10 global + 5 India) and writes structured JSON files to /news-cache/.
 *
 * Designed to run in GitHub Actions on a 4-hour cron schedule.
 * Zero npm dependencies — uses only Node.js built-in modules.
 *
 * API Reference: https://newsdata.io/documentation/#latest-news
 *
 * Free plan constraints:
 *   - 200 credits/day, 10 articles per credit
 *   - 30 credits per 15 minutes
 *   - 12hr delayed news
 *   - Max size=10 per request
 *   - No full_content, ai_tag, sentiment, ai_summary
 */

const https = require('https');
const fs = require('fs');
const path = require('path');

// ─────────────────────────────────────────────
// Configuration
// ─────────────────────────────────────────────

const NEWSDATA_KEY_GLOBAL = process.env.NEWSDATA_KEY_GLOBAL;
const NEWSDATA_KEY_INDIA = process.env.NEWSDATA_KEY_INDIA;

const BASE_URL = 'https://newsdata.io/api/1/latest';
const REQUEST_TIMEOUT_MS = 10000;     // 10 seconds
const DELAY_BETWEEN_REQUESTS_MS = 3000; // 3 seconds between each request
const RETRY_WAIT_ON_429_MS = 60000;   // 60 seconds wait on rate limit
const MAX_ARTICLE_AGE_HOURS = 48;     // Drop articles older than this
const MAX_ARTICLES_PER_FILE = 50;     // Max articles kept per category file
const CACHE_DIR = path.join(process.cwd(), 'news-cache');

/**
 * Feed definitions for all 15 categories.
 *
 * Query parameters per the NewsData.io documentation:
 *   - apikey:           Authentication (required)
 *   - language=en:      English articles only
 *   - category:         News category filter
 *   - removeduplicate=1: API-level deduplication (free tier)
 *   - prioritydomain=top: Prioritize top 10% sources (BBC, CNN, Reuters, etc.)
 *   - size=10:          Maximum articles per request (free plan cap)
 *   - image=1:          Only return articles that have an image
 *   - country=in:       India filter (India feeds only)
 *
 * Fields we use from the response (all free tier):
 *   article_id, title, link, description, image_url,
 *   source_icon, source_name, pubDate, category, country, language
 *
 * Fields we do NOT use (paywalled):
 *   content, ai_tag, sentiment, ai_summary, ai_org
 */
const FEEDS = [
  // ── Account 1: Global (no country filter) ──
  { key: 'global', prefix: 'global', category: 'technology' },
  { key: 'global', prefix: 'global', category: 'business' },
  { key: 'global', prefix: 'global', category: 'science' },
  { key: 'global', prefix: 'global', category: 'sports' },
  { key: 'global', prefix: 'global', category: 'entertainment' },
  { key: 'global', prefix: 'global', category: 'health' },
  { key: 'global', prefix: 'global', category: 'environment' },
  { key: 'global', prefix: 'global', category: 'politics' },
  { key: 'global', prefix: 'global', category: 'crime' },
  { key: 'global', prefix: 'global', category: 'world' },

  // ── Account 2: India (country=in on every request) ──
  { key: 'india', prefix: 'india', category: 'top' },
  { key: 'india', prefix: 'india', category: 'politics' },
  { key: 'india', prefix: 'india', category: 'sports' },
  { key: 'india', prefix: 'india', category: 'business' },
  { key: 'india', prefix: 'india', category: 'entertainment' },
  { key: 'india', prefix: 'india', category: 'technology' },
];

// ─────────────────────────────────────────────
// Utility Functions
// ─────────────────────────────────────────────

/**
 * Promise-based sleep.
 * @param {number} ms - Milliseconds to wait
 */
function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/**
 * HTTP GET with timeout. Returns parsed JSON.
 * Uses Node.js built-in https module — zero dependencies.
 *
 * @param {string} url - Full URL to fetch
 * @param {number} timeoutMs - Request timeout in milliseconds
 * @returns {Promise<{statusCode: number, body: object}>}
 */
function fetchWithTimeout(url, timeoutMs) {
  return new Promise((resolve, reject) => {
    const req = https.get(url, (res) => {
      let data = '';

      res.on('data', (chunk) => {
        data += chunk;
      });

      res.on('end', () => {
        try {
          const parsed = JSON.parse(data);
          resolve({ statusCode: res.statusCode, body: parsed });
        } catch (err) {
          reject(new Error(`Failed to parse JSON response: ${err.message}`));
        }
      });
    });

    req.on('error', (err) => {
      reject(new Error(`Network error: ${err.message}`));
    });

    req.setTimeout(timeoutMs, () => {
      req.destroy();
      reject(new Error(`Request timed out after ${timeoutMs}ms`));
    });
  });
}

/**
 * Normalize description field.
 * The API can return null, empty string "", or the literal string "null".
 * All three should become JSON null.
 *
 * @param {*} desc - Raw description value from API
 * @returns {string|null}
 */
function normalizeDescription(desc) {
  if (desc === null || desc === undefined) return null;
  if (typeof desc === 'string') {
    const trimmed = desc.trim();
    if (trimmed === '' || trimmed.toLowerCase() === 'null') return null;
    return trimmed;
  }
  return null;
}

/**
 * Check if an article's pubDate is within the last 48 hours.
 *
 * @param {string} dateStr - ISO 8601 date string from API
 * @returns {boolean}
 */
function isWithin48Hours(dateStr) {
  if (!dateStr) return false;
  try {
    const articleDate = new Date(dateStr);
    if (isNaN(articleDate.getTime())) return false;
    const cutoff = Date.now() - MAX_ARTICLE_AGE_HOURS * 60 * 60 * 1000;
    return articleDate.getTime() >= cutoff;
  } catch {
    return false;
  }
}

/**
 * Transform a raw API article object into our clean schema.
 * Only extracts free-tier fields.
 *
 * API response fields reference:
 *   https://newsdata.io/documentation/#latest-news-response
 *
 * @param {object} raw - Raw article object from NewsData.io results array
 * @returns {object} Cleaned article object
 */
function transformArticle(raw) {
  return {
    id: raw.article_id || null,
    title: raw.title || null,
    description: normalizeDescription(raw.description),
    url: raw.link || null,
    image_url: raw.image_url || null,
    source_icon: raw.source_icon || null,
    source: raw.source_name || raw.source_id || null,
    published_at: raw.pubDate || null,
    language: raw.language || 'en',
    category: Array.isArray(raw.category) ? raw.category[0] : (raw.category || null),
  };
}

/**
 * Deduplicate articles by article_id.
 * Keeps the first occurrence of each unique ID.
 *
 * @param {object[]} articles - Array of transformed article objects
 * @returns {object[]} Deduplicated array
 */
function deduplicateArticles(articles) {
  const seen = new Set();
  const unique = [];

  for (const article of articles) {
    if (article.id && !seen.has(article.id)) {
      seen.add(article.id);
      unique.push(article);
    }
  }

  return unique;
}

/**
 * Build the full API URL for a feed.
 *
 * Constructs URL with these query parameters:
 *   - apikey:            API key for authentication
 *   - language=en:       English language filter
 *   - category:          News category
 *   - removeduplicate=1: Remove duplicate articles (free tier feature)
 *   - prioritydomain=top: Prioritize top-tier news sources
 *   - size=10:           Max results per request (free plan limit)
 *   - image=1:           Only articles with images
 *   - country=in:        Country filter (India feeds only)
 *
 * @param {object} feed - Feed configuration object
 * @returns {string} Full API URL
 */
function buildUrl(feed) {
  const apiKey = feed.key === 'india' ? NEWSDATA_KEY_INDIA : NEWSDATA_KEY_GLOBAL;

  const params = new URLSearchParams({
    apikey: apiKey,
    language: 'en',
    category: feed.category,
    removeduplicate: '1',
    prioritydomain: 'top',
    size: '10',
    image: '1',
  });

  if (feed.key === 'india') {
    // India feeds: only Indian sources
    params.set('country', 'in');
  } else {
    // Global feeds: exclude India to avoid pollution from
    // high-volume Indian English-language publishers
    params.set('excludecountry', 'in');
  }

  return `${BASE_URL}?${params.toString()}`;
}

// ─────────────────────────────────────────────
// Core Fetch Logic
// ─────────────────────────────────────────────

/**
 * Fetch articles for a single category feed.
 *
 * Handles:
 *   - HTTP 200 with status:"error" in body → treated as failure
 *   - HTTP 429 rate limit → wait 60s, retry once, then skip
 *   - Network timeouts → skip
 *   - 0 results → returns null (caller preserves existing file)
 *
 * @param {object} feed - Feed configuration object
 * @returns {Promise<object[]|null>} Array of articles, or null on failure/empty
 */
async function fetchCategory(feed) {
  const feedName = `${feed.prefix}_${feed.category}`;
  const url = buildUrl(feed);

  // Mask API key in logs for security
  const safeUrl = url.replace(/apikey=[^&]+/, 'apikey=***');
  console.log(`  Fetching: ${feedName} → ${safeUrl}`);

  let response;
  try {
    response = await fetchWithTimeout(url, REQUEST_TIMEOUT_MS);
  } catch (err) {
    console.error(`  ✗ ${feedName}: ${err.message}`);
    return null;
  }

  // Handle HTTP 429 rate limit — wait 60s, retry once
  if (response.statusCode === 429) {
    console.warn(`  ⚠ ${feedName}: Rate limited (429). Waiting 60s before retry...`);
    await sleep(RETRY_WAIT_ON_429_MS);

    try {
      response = await fetchWithTimeout(url, REQUEST_TIMEOUT_MS);
    } catch (err) {
      console.error(`  ✗ ${feedName}: Retry failed: ${err.message}`);
      return null;
    }

    // Still 429 after retry — give up
    if (response.statusCode === 429) {
      console.error(`  ✗ ${feedName}: Still rate limited after retry. Skipping.`);
      return null;
    }
  }

  // Handle non-200 HTTP status
  if (response.statusCode !== 200) {
    console.error(`  ✗ ${feedName}: HTTP ${response.statusCode}`);
    return null;
  }

  // Check API-level status field (API can return 200 with status:"error")
  const body = response.body;
  if (body.status !== 'success') {
    const errorMsg = body.results?.message || body.results?.code || JSON.stringify(body);
    console.error(`  ✗ ${feedName}: API error — ${errorMsg}`);
    return null;
  }

  // Extract and validate results array
  const results = body.results;
  if (!Array.isArray(results) || results.length === 0) {
    console.warn(`  ⚠ ${feedName}: 0 articles returned. Keeping existing file.`);
    return null;
  }

  // Transform, filter stale articles, and deduplicate
  const articles = results
    .map(transformArticle)
    .filter((a) => a.id && a.title) // Must have at least an ID and title
    .filter((a) => isWithin48Hours(a.published_at));

  const deduped = deduplicateArticles(articles);

  if (deduped.length === 0) {
    console.warn(`  ⚠ ${feedName}: All articles filtered out (stale/invalid). Keeping existing file.`);
    return null;
  }

  console.log(`  ✓ ${feedName}: ${deduped.length} articles`);
  return deduped;
}

// ─────────────────────────────────────────────
// File Writing
// ─────────────────────────────────────────────

/**
 * Read existing articles from a category file on disk.
 * Returns empty array if file doesn't exist or is corrupted.
 *
 * @param {string} filePath - Absolute path to the JSON file
 * @returns {object[]} Existing articles array
 */
function readExistingArticles(filePath) {
  try {
    if (fs.existsSync(filePath)) {
      const raw = JSON.parse(fs.readFileSync(filePath, 'utf-8'));
      if (Array.isArray(raw.articles)) {
        return raw.articles;
      }
    }
  } catch (err) {
    console.warn(`  ⚠ Could not read existing file: ${err.message}`);
  }
  return [];
}

/**
 * Write a category JSON file to the cache directory.
 *
 * Merges new articles with existing ones on disk:
 *   1. Load existing articles from the file (if any)
 *   2. Combine new + existing (new articles first for dedup priority)
 *   3. Deduplicate by article_id
 *   4. Drop articles older than 48 hours
 *   5. Sort by published_at (newest first)
 *   6. Cap at MAX_ARTICLES_PER_FILE (50)
 *
 * This means articles accumulate over multiple runs (every 4 hours)
 * instead of being replaced. Stale articles naturally age out.
 *
 * @param {string} feedName - e.g. "global_technology"
 * @param {string} category - e.g. "technology"
 * @param {object[]} newArticles - Newly fetched articles from this run
 */
function writeCategoryFile(feedName, category, newArticles) {
  const now = new Date().toISOString();
  const filePath = path.join(CACHE_DIR, `${feedName}.json`);

  // Step 1: Load existing articles
  const existingArticles = readExistingArticles(filePath);

  // Step 2: Merge — new articles first (so dedup keeps newer version if ID matches)
  const combined = [...newArticles, ...existingArticles];

  // Step 3: Deduplicate by article_id
  const deduped = deduplicateArticles(combined);

  // Step 4: Drop stale articles (older than 48 hours)
  const fresh = deduped.filter((a) => isWithin48Hours(a.published_at));

  // Step 5: Sort by published_at (newest first)
  fresh.sort((a, b) => {
    const dateA = new Date(a.published_at || 0).getTime();
    const dateB = new Date(b.published_at || 0).getTime();
    return dateB - dateA;
  });

  // Step 6: Cap at max articles per file
  const capped = fresh.slice(0, MAX_ARTICLES_PER_FILE);

  const data = {
    category: feedName,
    last_updated: now,
    article_count: capped.length,
    articles: capped,
  };

  fs.writeFileSync(filePath, JSON.stringify(data, null, 2), 'utf-8');

  const newCount = newArticles.length;
  const keptFromExisting = capped.length - newArticles.filter((a) => capped.some((c) => c.id === a.id)).length;
  console.log(`  📄 Written: ${feedName}.json (${capped.length} total — ${newCount} new, merged with existing)`);
}

/**
 * Write the manifest.json file.
 * Only called after ALL category files have been successfully written.
 *
 * @param {object[]} fileEntries - Array of {name, last_updated, article_count}
 */
function writeManifest(fileEntries) {
  const now = new Date().toISOString();
  const filePath = path.join(CACHE_DIR, 'manifest.json');

  const manifest = {
    last_updated: now,
    files: fileEntries,
  };

  fs.writeFileSync(filePath, JSON.stringify(manifest, null, 2), 'utf-8');
  console.log(`\n  📋 Manifest written with ${fileEntries.length} files`);
}

// ─────────────────────────────────────────────
// Main Orchestrator
// ─────────────────────────────────────────────

async function main() {
  console.log('═══════════════════════════════════════════');
  console.log('  NewsData.io Cache Fetcher');
  console.log(`  Started: ${new Date().toISOString()}`);
  console.log('═══════════════════════════════════════════\n');

  // Validate API keys
  if (!NEWSDATA_KEY_GLOBAL) {
    console.error('ERROR: NEWSDATA_KEY_GLOBAL environment variable is not set.');
    process.exit(1);
  }
  if (!NEWSDATA_KEY_INDIA) {
    console.error('ERROR: NEWSDATA_KEY_INDIA environment variable is not set.');
    process.exit(1);
  }

  // Ensure cache directory exists
  if (!fs.existsSync(CACHE_DIR)) {
    fs.mkdirSync(CACHE_DIR, { recursive: true });
    console.log(`Created cache directory: ${CACHE_DIR}\n`);
  }

  // ── Phase 1: Fetch all feeds ──
  console.log('Phase 1: Fetching all feeds...\n');

  const results = []; // { feedName, category, articles }
  let successCount = 0;
  let failCount = 0;

  for (let i = 0; i < FEEDS.length; i++) {
    const feed = FEEDS[i];
    const feedName = `${feed.prefix}_${feed.category}`;

    const articles = await fetchCategory(feed);

    if (articles !== null) {
      results.push({ feedName, category: feed.category, articles });
      successCount++;
    } else {
      failCount++;
    }

    // Rate limit safety: wait between requests (skip delay after last request)
    if (i < FEEDS.length - 1) {
      await sleep(DELAY_BETWEEN_REQUESTS_MS);
    }
  }

  console.log(`\nFetch summary: ${successCount} succeeded, ${failCount} failed/skipped`);

  // ── Phase 2: Write files ──
  // If ALL requests failed, skip everything — no commit will happen
  if (results.length === 0) {
    console.log('\nAll requests failed. No files written. Exiting.');
    process.exit(0); // Exit 0 so the workflow doesn't fail — just no commit
  }

  console.log('\nPhase 2: Writing category files...\n');

  const manifestEntries = [];

  for (const result of results) {
    try {
      writeCategoryFile(result.feedName, result.category, result.articles);

      manifestEntries.push({
        name: `${result.feedName}.json`,
        last_updated: new Date().toISOString(),
        article_count: result.articles.length,
      });
    } catch (err) {
      console.error(`  ✗ Failed to write ${result.feedName}.json: ${err.message}`);
    }
  }

  // ── Phase 3: Write manifest (only after ALL files written) ──
  if (manifestEntries.length > 0) {
    // Also include existing files that weren't updated this run
    // (categories that returned 0 results or failed — their files are still on disk)
    const existingFiles = fs.readdirSync(CACHE_DIR)
      .filter((f) => f.endsWith('.json') && f !== 'manifest.json');

    const updatedNames = new Set(manifestEntries.map((e) => e.name));

    for (const existingFile of existingFiles) {
      if (!updatedNames.has(existingFile)) {
        // Read the existing file to get its metadata
        try {
          const existingPath = path.join(CACHE_DIR, existingFile);
          const existingData = JSON.parse(fs.readFileSync(existingPath, 'utf-8'));
          manifestEntries.push({
            name: existingFile,
            last_updated: existingData.last_updated || 'unknown',
            article_count: existingData.article_count || 0,
          });
        } catch (err) {
          console.warn(`  ⚠ Could not read existing file ${existingFile}: ${err.message}`);
        }
      }
    }

    // Sort manifest entries alphabetically for consistency
    manifestEntries.sort((a, b) => a.name.localeCompare(b.name));

    writeManifest(manifestEntries);
  }

  console.log('\n═══════════════════════════════════════════');
  console.log(`  Completed: ${new Date().toISOString()}`);
  console.log(`  Files written: ${manifestEntries.length} categories + manifest`);
  console.log('═══════════════════════════════════════════');
}

// Run
main().catch((err) => {
  console.error(`\nFatal error: ${err.message}`);
  console.error(err.stack);
  process.exit(1);
});
