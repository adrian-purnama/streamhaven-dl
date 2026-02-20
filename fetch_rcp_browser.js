#!/usr/bin/env node
/**
 * Fetch an rcp (play.html) URL in a real browser via Puppeteer.
 * Used when the page is behind Cloudflare Turnstile; the browser solves the challenge.
 * Reads URL from argv[2]; writes one JSON line to stdout: { html, cookies } or { error }.
 * Exit 0 on success, 1 on failure.
 */

const url = process.argv[2];
if (!url) {
  console.log(JSON.stringify({ error: 'Missing URL argument' }));
  process.exit(1);
}

async function main() {
  let browser;
  try {
    const puppeteer = require('puppeteer');
    // Actual browser window (headless: false) so Turnstile can run and user can see progress
    browser = await puppeteer.launch({
      headless: false,
      args: ['--no-sandbox', '--disable-setuid-sandbox'],
      defaultViewport: { width: 1280, height: 720 },
    });
    const page = await browser.newPage();
    await page.setUserAgent(
      'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36'
    );
    await page.goto(url, { waitUntil: 'domcontentloaded', timeout: 60000 });

    // Poll until page content contains /prorcp/ (Turnstile solved, real content loaded)
    const maxWaitMs = 60000;
    const pollMs = 1000;
    let html = '';
    for (let elapsed = 0; elapsed < maxWaitMs; elapsed += pollMs) {
      html = await page.content();
      if (/\/prorcp\//.test(html)) break;
      await new Promise((r) => setTimeout(r, pollMs));
    }

    if (!/\/prorcp\//.test(html)) {
      console.log(JSON.stringify({ error: 'prorcp token never appeared (Turnstile timeout?)' }));
      process.exit(1);
    }

    const cookies = await page.cookies();
    const cookieList = cookies.map((c) => ({
      name: c.name,
      value: c.value,
      domain: c.domain || '',
      path: c.path || '/',
    }));

    // Single JSON line so Python can read with one readline()
    console.log(JSON.stringify({ html, cookies: cookieList }));
    process.exit(0);
  } catch (err) {
    console.log(JSON.stringify({ error: err.message || String(err) }));
    process.exit(1);
  } finally {
    if (browser) await browser.close();
  }
}

main();
