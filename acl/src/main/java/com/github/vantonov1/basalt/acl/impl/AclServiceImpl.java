package com.github.vantonov1.basalt.acl.impl;

import com.github.vantonov1.basalt.acl.AclService;
import com.github.vantonov1.basalt.cache.TransactionalCacheManager;
import com.github.vantonov1.basalt.repo.NodeService;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
@SuppressWarnings("unused")
class AclServiceImpl implements AclService {

    private static final String ACLS_CACHE = "ACLS";
    private static final List<ACE> EMPTY_ACES = new ArrayList<>();

    private final TransactionalCacheManager cacheManager;
    private final AclDAO aclDAO;
    private final NodeService nodeService;

    public AclServiceImpl(TransactionalCacheManager cacheManager, AclDAO aclDAO, NodeService nodeService) {
        this.cacheManager = cacheManager;
        this.aclDAO = aclDAO;
        this.nodeService = nodeService;
    }

    @Override
    public void addAcl(String id, Map<String, Integer> acl) {
        checkParam(id, "node id is null");
        checkParam(acl, "acl is null");
        if (!acl.isEmpty()) {
            createAces(id, acl);
        }
    }

    @Override
    public void setAcl(String id, Map<String, Integer> acl) {
        checkParam(id, "node id is null");
        removeAces(id);
        if (acl != null && !acl.isEmpty()) {
            createAces(id, acl);
        }
    }

    @Override
    public void setAcl(String id, String authority, Integer mask) {
        checkParam(id, "node id is null");
        checkParam(authority, "authority id is null");
        removeAces(id, authority);
        if (mask != null && mask != 0) {
            createAces(id, Collections.singletonMap(authority, mask));
        }
    }

    @Override
    public void removeACLs(String authority) {
        checkParam(authority, "authority id is null");
        removeAces(null, authority);
    }

    @Override
    public Map<String, Integer> getAcl(String id) {
        final List<ACE> aces = getInheritedAces(id);
        return aces != null ? convertToAcl(aces) : null;
    }

    @Override
    public boolean convertToReadonly(String id) {
        checkParam(id, "node id is null");
        final List<ACE> aces = getAces(id);
        if (aces != null) {
            convertAcesTo(id, MASK_READ);
        } else {
            createAces(id, EVERYONE_READ);
        }
        return aces != null;
    }

    @Override
    public boolean convertToReadWrite(String id) {
        checkParam(id, "node id is null");
        final List<ACE> aces = getAces(id);
        if (aces != null) {
            convertAcesTo(id, MASK_READ | MASK_WRITE);
        } else {
            createAces(id, EVERYONE_READ_WRITE);
        }
        return aces != null;
    }

    @Override
    public boolean isReadableBy(String id, String authority, Collection<String> membership) {
        return hasAccess(id, authority, membership, MASK_READ);
    }

    @Override
    public boolean isWritableBy(String id, String authority, Collection<String> membership) {
        return hasAccess(id, authority, membership, MASK_WRITE);
    }

    @Override
    public boolean isDeletableBy(String id, String authority, Collection<String> membership) {
        return hasAccess(id, authority, membership, MASK_DELETE);
    }

    @Override
    public List<String> filterReadable(Collection<String> ids, final String personId, Collection<String> membership) {
        if (ids == null) {
            return Collections.emptyList();
        }
        final List<ACE> aces = getAces(ids);
        if (aces == null || aces.isEmpty()) {
            return new ArrayList<>(parentsHasAccess(ids, personId, membership));
        }
        final List<String> result = new ArrayList<>();
        final Collection<String> left = new HashSet<>();
        for (String id : ids) {
            final Boolean match = hasAccess(personId, membership, MASK_READ, filteredById(id, aces));
            if (Boolean.TRUE.equals(match)) {
                result.add(id);
            } else if (match == null) {
                left.add(id);
            }
        }
        if (!left.isEmpty()) {
            final Collection<String> fromParents = parentsHasAccess(left, personId, membership);
            result.addAll(fromParents);
            left.removeAll(fromParents);
        }
        return result;

    }

    private Collection<String> parentsHasAccess(Collection<String> children, String authorityId, Collection<String> membership) {
        final Map<String, String> parents = nodeService.getPrimaryParents(children);
        if (!parents.isEmpty()) {
            final Set<String> result = new HashSet<>(children.size());
            for (String child : children) {
                if (!parents.keySet().contains(child)) {
                    result.add(child);
                }
            }
            final Collection<String> ids = parents.values();
            final List<ACE> aces = getAces(ids);
            final List<String> left;
            if (aces == null || aces.isEmpty()) {
                left = new ArrayList<>(ids);
            } else {
                left = new ArrayList<>(children.size());
                for (String id : ids) {
                    final Collection<ACE> nodeAces = filteredById(id, aces);
                    if (!nodeAces.isEmpty()) {
                        final Boolean match = hasAccess(authorityId, membership, MASK_READ, nodeAces);
                        if (Boolean.TRUE.equals(match)) {
                            for (Map.Entry<String, String> entry : parents.entrySet()) {
                                if (entry.getValue().equals(id)) {
                                    result.add(entry.getKey());
                                }
                            }
                        }
                    } else {
                        left.add(id);
                    }
                }
            }
            if (!left.isEmpty()) {
                final Collection<String> parentsHasAccess = parentsHasAccess(left, authorityId, membership);
                if (!parentsHasAccess.isEmpty()) {
                    for (String child : children) {
                        if (parentsHasAccess.contains(parents.get(child))) {
                            result.add(child);
                        }
                    }
                }
            }
            return result;
        } else {
            return children;
        }
    }


    private boolean hasAccess(String id, String authority, Collection<String> membership, int mask) {
        final List<ACE> aces = getInheritedAces(id);
        return aces == null || Boolean.TRUE.equals(hasAccess(authority, membership, mask, aces));
    }

    private Boolean hasAccess(String authority, Collection<String> membership, int mask, Iterable<ACE> aces) {
        final Boolean match = matchStrict(aces, authority, mask);
        if (match != null) {
            return match;
        }
        if (membership != null) {
            for (String group : membership) {
                final Boolean matchGroup = matchStrict(aces, group, mask);
                if (matchGroup != null) {
                    return matchGroup;
                }
            }
        }
        return matchStrict(aces, AUTHORITY_WORLD, mask);
    }

    private Boolean matchStrict(Iterable<ACE> aces, String sid, int permission) {
        if (sid != null && aces != null) {
            for (ACE entry : aces) {
                if (entry.authorityId.equals(sid)) {
                    return (entry.mask & permission) != 0;
                }
            }
        }
        return null;
    }

    private static Collection<ACE> filteredById(final String id, List<ACE> aces) {
        return aces != null ? aces.stream().filter(ace -> ace.nodeId.equals(id)).collect(Collectors.toList()) : null;
    }

    private List<ACE> getInheritedAces(String id) {
        while (id != null) {
            final List<ACE> aces = getAces(id);
            if (aces != null) {
                return aces;
            }
            id = nodeService.getPrimaryParent(id);
        }
        return null;
    }

    private List<ACE> getAces(String id) {
        if (id != null) {
            List<ACE> aces = cacheManager.get(ACLS_CACHE, id);
            if (aces == null) {
                aces = aclDAO.getAces(id);
                cacheManager.putExisting(ACLS_CACHE, id, aces != null ? aces : EMPTY_ACES);
            }
            return aces == EMPTY_ACES ? null : aces;
        } else {
            return Collections.emptyList();
        }
    }

    private List<ACE> getAces(Collection<String> ids) {
        if (ids != null && ids.iterator().hasNext()) {
            final Set<String> copy = new HashSet<>(ids);
            final List<ACE> result = new ArrayList<>(copy.size());
            for (Iterator<String> iterator = copy.iterator(); iterator.hasNext(); ) {
                final Collection<ACE> aces = cacheManager.get(ACLS_CACHE, iterator.next());
                if (aces != null) {
                    iterator.remove();
                    result.addAll(aces);
                }
            }
            if (!copy.isEmpty()) {
                final List<ACE> aces = aclDAO.getAces(copy);
                if (aces != null && !aces.isEmpty()) {
                    result.addAll(aces);
                    final Map<String, Collection<ACE>> acesById = new HashMap<>();
                    for (ACE ace : aces) {
                        acesById.computeIfAbsent(ace.nodeId, k -> new ArrayList<>()).add(ace);
                    }
                    for (String id : copy) {
                        final Collection<ACE> existing = acesById.get(id);
                        final List<ACE> value = existing != null && !existing.isEmpty() ? new ArrayList<>(existing) : EMPTY_ACES;
                        cacheManager.putExisting(ACLS_CACHE, id, value);
                    }
                } else {
                    for (String id : copy) {
                        cacheManager.putExisting(ACLS_CACHE, id, EMPTY_ACES);
                    }
                }
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    private static Map<String, Integer> convertToAcl(List<ACE> aces) {
        if (aces != null) {
            final Map<String, Integer> result = new HashMap<>(aces.size());
            for (ACE entry : aces) {
                result.put(entry.authorityId, entry.mask);
            }
            return result;

        }
        return null;
    }

    private static Collection<ACE> convertToAces(Map<String, Integer> acl) {
        if (acl != null && !acl.isEmpty()) {
            final List<ACE> aces = new ArrayList<>();
            for (Map.Entry<String, Integer> entry : acl.entrySet()) {
                aces.add(new ACE(null, entry.getKey(), entry.getValue()));
            }

            return aces;
        }
        return Collections.emptyList();
    }

    private void createAces(String id, Map<String, Integer> acl) {
        if (acl != null && !acl.isEmpty()) {
            aclDAO.createAces(id, convertToAces(acl));
            cacheManager.markAsCreated(ACLS_CACHE, id);
        }
    }

    private void removeAces(String id) {
        aclDAO.removeAces(id, null);
        cacheManager.remove(ACLS_CACHE, id);
    }

    private void removeAces(String id, String authority) {
        aclDAO.removeAces(id, authority);
        cacheManager.remove(ACLS_CACHE, id);
    }

    private void convertAcesTo(String id, int mask) {
        aclDAO.convertAcesTo(id, mask);
        cacheManager.remove(ACLS_CACHE, id);
    }

    private static void checkParam(Object id, String msg) {
        if (id == null) {
            throw new IllegalArgumentException(msg);
        }
    }

}
