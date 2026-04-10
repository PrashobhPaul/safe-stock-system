/**
 * ProfitPilot Service Worker v5
 * Bumped from v4 to force eviction of stale index.html on returning users.
 * Strategy: stale-while-revalidate for predictions.json; network-first for
 * the app shell so new builds propagate without a hard reload.
 */

const CACHE_NAME  = 'profitpilot-v5';
const DATA_CACHE  = 'profitpilot-data-v5';
const OFFLINE_URL = './offline.html';

const APP_SHELL = [
  './',
  './index.html',
  './manifest.json',
  './offline.html',
  './icons/icon-192.png',
  './icons/icon-512.png',
];

// ── INSTALL ──
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(APP_SHELL))
      .then(() => self.skipWaiting())
  );
});

// ── ACTIVATE — evict all old caches ──
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys.filter(k => k !== CACHE_NAME && k !== DATA_CACHE)
            .map(k => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

// ── FETCH ──
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);
  if (event.request.method !== 'GET') return;
  if (url.protocol === 'chrome-extension:') return;

  // predictions.json → stale-while-revalidate
  if (url.pathname.endsWith('predictions.json')) {
    event.respondWith(staleWhileRevalidate(event.request, DATA_CACHE));
    return;
  }

  // index.html and root → network first so fresh builds propagate
  if (url.pathname.endsWith('/') || url.pathname.endsWith('index.html')) {
    event.respondWith(networkFirst(event.request, CACHE_NAME));
    return;
  }

  // Fonts & static assets → cache first
  if (url.hostname.includes('fonts.g')) {
    event.respondWith(cacheFirst(event.request, CACHE_NAME));
    return;
  }

  event.respondWith(cacheFirst(event.request, CACHE_NAME));
});

// ── STRATEGIES ──
async function cacheFirst(request, cacheName) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    return caches.match(OFFLINE_URL) || new Response('Offline', { status: 503 });
  }
}

async function networkFirst(request, cacheName) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    return cached || caches.match(OFFLINE_URL);
  }
}

async function staleWhileRevalidate(request, cacheName) {
  const cache  = await caches.open(cacheName);
  const cached = await cache.match(request);
  const fetchPromise = fetch(request).then(response => {
    if (response.ok) cache.put(request, response.clone());
    return response;
  }).catch(() => null);
  return cached || await fetchPromise || new Response('{}', {
    headers: { 'Content-Type': 'application/json' }
  });
}
