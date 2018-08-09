package com.github.vantonov1.basalt.cache;

import org.springframework.cache.Cache;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class TransactionalCache<T> extends TransactionSynchronizationAdapter {
    private final String cacheName;
    private final Set<String> l2Changed = Collections.newSetFromMap(new ConcurrentHashMap<>());
    private final Cache l2Cache;


    public TransactionalCache(String cacheName, Cache cache) {
        this.cacheName = cacheName;
        this.l2Cache = cache;
    }

    @SuppressWarnings("unchecked")
    public T get(String id) {
        if (id != null) {
            T local = getFromL1(id);
            if (local == null && !isMarkedAsChanged(id)) {
                final Cache.ValueWrapper wrapper = l2Cache.get(id);
                return wrapper != null ? (T) wrapper.get() : null;
            }
            return local;
        } else {
            return null;
        }
    }

    public void putExisting(String id, T value) {
        if (id != null && value != null) {
            putToL1(id, value);
            if (isReadonlyTX() || !isMarkedAsChanged(id)) {
                l2Cache.put(id, value);
            }
        }
    }

    public void remove(String id) {
        if (id != null) {
            assert !isReadonlyTX();
            removeFromL1(id);
            markAsChanged(id);
        }
    }

    public boolean isCached(String id) {
        if (id != null) {
            final Map<String, T> l1 = getL1Cache();
            return (l1 != null && l1.containsKey(id)) || (!l2Changed.contains(id) && (l2Cache.get(id) != null));
        }
        return false;
    }

    public void clear() {
        unbindResources();
        l2Cache.clear();
    }

    @Override
    public void afterCommit() {
        final Set<String> changes = getLocalChanges();
        if (changes != null) {
            for (String id : changes) {
                l2Cache.evict(id);
            }
        }
    }

    @Override
    public void afterCompletion(int status) {
        final Set<String> local = getLocalChanges();
        if (local != null) {
            for (String id : local) {
                l2Changed.remove(id);
            }
        }
        unbindResources();
    }

    private T getFromL1(String id) {
        final Map<String, T> l1 = getL1Cache();
        return l1 != null ? l1.get(id) : null;
    }

    private void putToL1(String id, T value) {
        Map<String, T> l1 = getL1Cache();
        if (l1 == null) {
            bindResources();
            l1 = getL1Cache();
            assert l1 != null;
        }
        l1.put(id, value);
    }

    private void removeFromL1(String id) {
        Map<String, T> l1 = getL1Cache();
        if (l1 != null) {
            l1.remove(id);
        }
    }

    private boolean isMarkedAsChanged(String id) {
        final Set<String> localChanges = getLocalChanges();
        return localChanges != null && localChanges.contains(id);
    }


    private void markAsChanged(String id) {
        Set<String> localChanges = getLocalChanges();
        if (localChanges == null) {
            bindResources();
            localChanges = getLocalChanges();
            assert localChanges != null;
        }
        if (localChanges.add(id) && !l2Changed.add(id)) {
            throw new OptimisticLockingFailureException("already in cache");
        }
    }

    private boolean isReadonlyTX() {
        return TransactionSynchronizationManager.isCurrentTransactionReadOnly();
    }

    @SuppressWarnings("unchecked")
    private Map<String, T> getL1Cache() {
        return (Map<String, T>) TransactionSynchronizationManager.getResource(cacheName + "L1Cache");
    }

    @SuppressWarnings("unchecked")
    private Set<String> getLocalChanges() {
        return (Set<String>) TransactionSynchronizationManager.getResource(cacheName + "localChanges");
    }

    private void bindResources() {
//        assert TransactionSynchronizationManager.isActualTransactionActive() : "transaction not started";
        TransactionSynchronizationManager.bindResource(cacheName + "localChanges", new HashSet<>());
        TransactionSynchronizationManager.bindResource(cacheName + "L1Cache", new HashMap<>());
        if(TransactionSynchronizationManager.isActualTransactionActive()) {
            TransactionSynchronizationManager.registerSynchronization(this);
        }
    }

    private void unbindResources() {
        final String L1Cache = cacheName + "L1Cache";
        if (TransactionSynchronizationManager.hasResource(L1Cache)) {
            TransactionSynchronizationManager.unbindResource(L1Cache);
        }
        final String localChanges = cacheName + "localChanges";
        if (TransactionSynchronizationManager.hasResource(localChanges)) {
            TransactionSynchronizationManager.unbindResource(localChanges);
        }
    }
}
