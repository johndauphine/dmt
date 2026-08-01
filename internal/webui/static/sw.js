"use strict";
/* dmt WebUI service worker. Caches only the static shell so the app still
   loads offline/instantly; it must NEVER cache /api/ — a stale cached
   /api/status or /api/run would silently show a wrong migration state.

   __DMT_VERSION__ is substituted server-side (assets.go) with the running
   dmt build's version, so upgrading dmt changes the cache name and the old
   cache is dropped in activate below. Without this, an operator who upgrades
   dmt could keep being served yesterday's app.js from the browser cache
   indefinitely.

   This is a pure enhancement: registration is wrapped in a feature-detect +
   .catch() in app.js, and nothing here is load-bearing — a browser that
   refuses to register a service worker (older Safari has historically been
   inconsistent on http://localhost) must still get a fully working app. */

const CACHE = "dmt-shell-__DMT_VERSION__";
const SHELL = [
  "/",
  "/app.css",
  "/app.js",
  "/manifest.webmanifest",
  "/icon-192.png",
  "/icon-512.png",
  "/icon-512-maskable.png",
  "/apple-touch-icon.png",
];

self.addEventListener("install", (event) => {
  event.waitUntil(
    caches.open(CACHE).then((cache) => cache.addAll(SHELL)).then(() => self.skipWaiting())
  );
});

self.addEventListener("activate", (event) => {
  event.waitUntil(
    caches.keys()
      .then((keys) => Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k))))
      .then(() => self.clients.claim())
  );
});

self.addEventListener("fetch", (event) => {
  const url = new URL(event.request.url);
  if (event.request.method !== "GET" || url.pathname.startsWith("/api/")) {
    return; // network-only: never intercept the API or SSE stream
  }
  event.respondWith(
    caches.match(event.request).then((cached) => cached || fetch(event.request))
  );
});
