/**
 * ProfitPilot Service Worker — KILL SWITCH
 *
 * This SW does nothing except unregister itself and delete all caches.
 * Deploy this FIRST for a few hours/days to evict all clients that are
 * stuck on old broken versions (v4/v5 deadlock, v6 infinite loading, etc).
 *
 * Once metrics confirm most clients are cleaned up, replace with the
 * real SW again (v7+).
 *
 * Why this works:
 *   - GitHub Pages always re-checks sw.js on page load (default HTTP cache
 *     on sw.js is ~24h but most browsers bypass this after updatefound).
 *   - When the browser fetches this new sw.js, it sees a byte-different
 *     file from the cached one, triggers update, installs, activates,
 *     and immediately unregisters + evicts everything.
 *   - Page navigations from that point on go direct to network.
 *
 * The page-level self-heal (in index.html) handles the one-time reload.
 */

self.addEventListener('install', event => {
  self.skipWaiting();
});

self.addEventListener('activate', event => {
  event.waitUntil((async () => {
    // Nuke every cache
    const keys = await caches.keys();
    await Promise.all(keys.map(k => caches.delete(k)));

    // Unregister self
    await self.registration.unregister();

    // Force every open client to reload into a SW-less state
    const clients = await self.clients.matchAll({ type: 'window' });
    for (const client of clients) {
      client.navigate(client.url).catch(() => {});
    }
  })());
});

// Pass every fetch straight through — no caching, no interception
self.addEventListener('fetch', event => {
  // Deliberately do nothing. Browser falls back to default network behavior.
});
