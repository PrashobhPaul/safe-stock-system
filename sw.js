/**
 * StockSage India — Service Worker v4.0
 * Caching strategy:
 *   App shell (HTML/CSS/fonts) → Cache First (instant load)
 *   predictions.json           → Stale-While-Revalidate (show cached, update in background)
 *   Live Vercel API            → Network First with cache fallback
 */

const CACHE_NAME    = 'stocksage-v4';
const DATA_CACHE    = 'stocksage-data-v4';
const OFFLINE_URL   = './offline.html';

// App shell — these files are cached on install for offline use
const APP_SHELL = [
  './',
  './index.html',
  './manifest.json',
  './offline.html',
  './icons/icon-192.png',
  './icons/icon-512.png',
  'https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Crimson+Pro:ital,wght@0,400;0,600;1,400&family=DM+Sans:wght@300;400;500;600&display=swap',
];

// ── INSTALL ──
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME)
      .then(cache => cache.addAll(APP_SHELL))
      .then(() => self.skipWaiting())
  );
});

// ── ACTIVATE ──
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(k => k !== CACHE_NAME && k !== DATA_CACHE)
          .map(k => caches.delete(k))
      )
    ).then(() => self.clients.claim())
  );
});

// ── FETCH ──
self.addEventListener('fetch', event => {
  const url = new URL(event.request.url);

  // Skip non-GET requests
  if (event.request.method !== 'GET') return;

  // Skip Chrome extension requests
  if (url.protocol === 'chrome-extension:') return;

  // predictions.json → Stale-While-Revalidate
  if (url.pathname.endsWith('predictions.json')) {
    event.respondWith(staleWhileRevalidate(event.request, DATA_CACHE));
    return;
  }

  // Vercel API → Network First, fall back to cache
  if (url.hostname.includes('vercel.app')) {
    event.respondWith(networkFirstWithCache(event.request, DATA_CACHE));
    return;
  }

  // Google Fonts → Cache First
  if (url.hostname.includes('fonts.googleapis.com') || url.hostname.includes('fonts.gstatic.com')) {
    event.respondWith(cacheFirst(event.request, CACHE_NAME));
    return;
  }

  // App shell → Cache First, then network
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
    return new Response('Offline', { status: 503 });
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

async function networkFirstWithCache(request, cacheName) {
  try {
    const response = await fetch(request, { signal: AbortSignal.timeout(8000) });
    if (response.ok) {
      const cache = await caches.open(cacheName);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    return cached || new Response(JSON.stringify({ error: 'Offline' }), {
      headers: { 'Content-Type': 'application/json' }
    });
  }
}

// ── BACKGROUND SYNC — refresh data when back online ──
self.addEventListener('sync', event => {
  if (event.tag === 'sync-predictions') {
    event.waitUntil(
      caches.open(DATA_CACHE).then(cache =>
        fetch('./predictions.json').then(r => {
          if (r.ok) cache.put('./predictions.json', r);
        })
      )
    );
  }
});

// ── PUSH NOTIFICATIONS (future use) ──
self.addEventListener('push', event => {
  if (!event.data) return;
  const data = event.data.json();
  event.waitUntil(
    self.registration.showNotification('StockSage India', {
      body:    data.body || 'New picks available',
      icon:    './icons/icon-192.png',
      badge:   './icons/icon-96.png',
      vibrate: [200, 100, 200],
      data:    { url: data.url || './' },
    })
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(clients.openWindow(event.notification.data.url));
});
