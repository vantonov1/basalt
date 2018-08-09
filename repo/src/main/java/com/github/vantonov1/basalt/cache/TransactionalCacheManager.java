package com.github.vantonov1.basalt.cache;

import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Supports transaction-aware L1+L2 caching. Uses Spring Boot <a href="https://docs.spring.io/spring-boot/docs/current/reference/html/boot-features-caching.html">caching</a> for L2 cache
 * <p>If no {@link CacheManager} is defined in context, uses {@link org.springframework.cache.concurrent.ConcurrentMapCacheManager ConcurrentMapCacheManager} as default</p>
 */
@Component
public class TransactionalCacheManager {
    private final CacheManager l2CacheManager;
    private final Map<String, TransactionalCache> caches = new ConcurrentHashMap<>();

    public TransactionalCacheManager(CacheManager l2CacheManager) {
        this.l2CacheManager = l2CacheManager;
    }

    public boolean contains(String cacheName, String key) {
        final TransactionalCache cache = caches.get(cacheName);
        return cache != null && cache.isCached(key);
    }

    @SuppressWarnings("unchecked")
    public <V> V get(String cacheName, String key) {
        final TransactionalCache<V> cache = caches.get(cacheName);
        return cache != null ? cache.<V>get(key) : null;
    }

    public <V> void putExisting(String cacheName, String key, V value) {
        this.<V>createCache(cacheName).putExisting(key, value);
    }

    public void markAsCreated(String cacheName, String key) {
        remove(cacheName, key);
    }

    public void remove(String cacheName, String key) {
        final TransactionalCache cache = caches.get(cacheName);
        if (cache != null) {
            cache.remove(key);
        }
    }

    public void clear(String cacheName) {
        final TransactionalCache cache = caches.get(cacheName);
        if (cache != null) {
            cache.clear();
        }
    }

    @SuppressWarnings("unchecked")
    private <V> TransactionalCache<V> createCache(String cacheName) {
        TransactionalCache<V> cache = caches.get(cacheName);
        if (cache == null) {
            synchronized (caches) {
                cache = caches.get(cacheName);
                if (cache == null) {
                    cache = new TransactionalCache<>(cacheName, l2CacheManager.getCache(cacheName));
                    caches.put(cacheName, cache);
                }
            }
        }
        return cache;
    }
}
