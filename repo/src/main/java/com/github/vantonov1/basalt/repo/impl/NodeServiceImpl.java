package com.github.vantonov1.basalt.repo.impl;

import com.github.vantonov1.basalt.cache.TransactionalCacheManager;
import com.github.vantonov1.basalt.repo.Assoc;
import com.github.vantonov1.basalt.repo.FullTextIndexer;
import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.NodeService;
import com.github.vantonov1.basalt.repo.QueryBuilder;
import com.github.vantonov1.basalt.repo.SearchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Service
@SuppressWarnings("unused")
public class NodeServiceImpl implements NodeService, SearchService {
    private static final String PARENTS_CACHE = "parents";
    private static final String NODES_CACHE = "nodes";
    private static final String CHILDREN_CACHE = "children";
    private static final String ASSOC_CACHE = "assocByType";

    private final TransactionalCacheManager cacheManager;
    private final RepositoryDAO repositoryDAO;

    private FullTextIndexer fullTextIndexer;

    public NodeServiceImpl(TransactionalCacheManager cacheManager, RepositoryDAO repositoryDAO) {
        this.cacheManager = cacheManager;
        this.repositoryDAO = repositoryDAO;
    }

    @Autowired(required = false)
    public void setFullTextIndexer(FullTextIndexer fullTextIndexer) {
        this.fullTextIndexer = fullTextIndexer;
    }

    @Override
    public String createNode(final String parentId, final Node node, final String assocType, String assocName) {
        checkParam(node, "nodes is null");
        checkNodeId(node);
        if (exists(node.id)) {
            throw new IllegalArgumentException("node id already exists: " + node.id);
        }
        final String id = repositoryDAO.createNode(node, parentId);
        if (parentId != null && assocType != null) {
            repositoryDAO.createAssoc(parentId, id, assocType, assocName);
            cacheManager.markAsCreated(CHILDREN_CACHE, parentId);
            cacheManager.markAsCreated(ASSOC_CACHE, assocType);
        }
        if (fullTextIndexer != null) {
            fullTextIndexer.create(id, node);
        }
        return id;
    }

    @Override
    public List<String> createNodes(String parentId, Collection<Node> nodes, String assocType) {
        checkParam(nodes, "nodes are null");
        if (!nodes.isEmpty()) {
            nodes.forEach(this::checkNodeId);
            if (exists(nodes.stream().map(node -> node.id).filter(id -> !Objects.isNull(id)).collect(Collectors.toList()))) {
                throw new IllegalArgumentException("node id already exists");
            }
            final List<String> ids = repositoryDAO.createNodes(nodes, parentId);
            assert ids.size() == nodes.size();
            if (parentId != null) {
                repositoryDAO.createAssocs(parentId, ids, assocType);
                cacheManager.markAsCreated(CHILDREN_CACHE, parentId);
                cacheManager.markAsCreated(ASSOC_CACHE, assocType);
            }
            if (fullTextIndexer != null) {
                fullTextIndexer.create(ids, nodes);
            }
            return ids;
        } else {
            return Collections.emptyList();
        }
    }

    @Override
    public Map<String, Collection<String>> createNodes(Map<String, Collection<Node>> nodesInParent, String assocType) {
        checkParam(nodesInParent, "nodes are null");
        final Map<String, Collection<String>> idsByParent = repositoryDAO.createNodes(nodesInParent);
        for (Map.Entry<String, Collection<String>> entry : idsByParent.entrySet()) {
            final String parentId = entry.getKey();
            final Collection<String> ids = entry.getValue();
            if (assocType != null) {
                repositoryDAO.createAssocs(parentId, ids, assocType);
            }
            if (fullTextIndexer != null) {
                fullTextIndexer.create(ids, nodesInParent.get(parentId));
            }
            cacheManager.markAsCreated(CHILDREN_CACHE, parentId);
        }
        if (assocType != null) {
            cacheManager.markAsCreated(ASSOC_CACHE, assocType);
        }
        return idsByParent;
    }

    @Override
    public void updateProperties(Node updated, boolean deleteOld) {
        checkParam(updated, "node is null");
        if (updated.id != null) {
            cacheManager.remove(NODES_CACHE, updated.id);
            repositoryDAO.updateNode(updated.id, updated, deleteOld);
            if (fullTextIndexer != null) {
                fullTextIndexer.update(updated, deleteOld);
            }
        }
    }

    @Override
    public void updateProperties(Collection<Node> nodes, boolean deleteOld) {
        checkParam(nodes, "nodes are null");
        if (!nodes.isEmpty()) {
            final List<String> ids = nodes.stream().map((Node node) -> node != null ? node.id : null).collect(Collectors.toList());
            repositoryDAO.updateProperties(nodes, getProperties(ids), deleteOld);
            ids.forEach(id -> cacheManager.remove(NODES_CACHE, id));
            if (fullTextIndexer != null) {
                fullTextIndexer.update(nodes, deleteOld);
            }
        }
    }

    @Override
    public void setProperty(final String id, final String name, final Serializable value) {
        checkParam(id, "node id is null");
        checkParam(name, "property name is null");
        final Serializable oldValue = getProperty(id, name);
        if (!Objects.equals(oldValue, value)) {
            final Node cached = cacheManager.get(NODES_CACHE, id);
            final Date modified = cached != null ? cached.modified : null;
            if (value == null) {
                repositoryDAO.removeProperty(id, name, modified);
            } else {
                repositoryDAO.setProperty(id, name, modified, value, oldValue);
            }
            cacheManager.remove(NODES_CACHE, id);
            if (fullTextIndexer != null) {
                fullTextIndexer.update(id, name, value instanceof String ? (String) value : null);
            }
        }
    }

    @Override
    public void setProperty(Collection<String> ids, String name, Serializable value) {
        checkParam(ids, "node ids are null");
        checkParam(name, "property name is null");
        if (!ids.isEmpty()) {
            if (value == null) {
                repositoryDAO.removeProperty(ids, name);
            } else if (ids.size() == 1) {
                setProperty(ids.iterator().next(), name, value);
            } else {
                repositoryDAO.setProperty(ids, name, value, getProperty(ids, name));
            }
            for (String id : ids) {
                cacheManager.remove(NODES_CACHE, id);
            }
            if (fullTextIndexer != null) {
                for (String id : ids) {
                    fullTextIndexer.update(id, name, value instanceof String ? (String) value : null);
                }
            }
        }
    }

    @Override
    public void removeProperty(String id, String name) {
        checkParam(id, "node id is null");
        checkParam(name, "property name is null");
        final Node cached = cacheManager.get(NODES_CACHE, id);
        repositoryDAO.removeProperty(id, name, cached != null ? cached.modified : null);
        cacheManager.remove(NODES_CACHE, id);
        if (fullTextIndexer != null) {
            fullTextIndexer.update(id, name, null);
        }
    }

    @Override
    public <T extends Serializable> T getProperty(final String id, final String name) {
        if (id != null && name != null) {
            final Node cached = cacheManager.get(NODES_CACHE, id);
            return cached != null ? cached.get(name) : repositoryDAO.getProperty(id, name);
        }
        return null;
    }

    @Override
    public Map<String, Object> getProperty(final Collection<String> ids, final String name) {
        if (ids != null && !ids.isEmpty() && name != null) {
            final List<String> uncached = new ArrayList<>();
            final Map<String, Object> result = new HashMap<>();
            for (final String id : ids) {
                final Node cached = cacheManager.get(NODES_CACHE, id);
                if (cached != null) {
                    result.put(id, cached.get(name));
                } else {
                    uncached.add(id);
                }
            }
            if (result.isEmpty()) {
                return repositoryDAO.getProperty(ids, name);
            } else if (!uncached.isEmpty()) {
                result.putAll(repositoryDAO.getProperty(uncached, name));
            }
            return result;
        }
        return Collections.emptyMap();
    }

    @Override
    public Node getProperties(final String id) {
        if (id != null) {
            final Node cached = cacheManager.get(NODES_CACHE, id);
            if (cached != null) {
                return cached;
            } else {
                final Node node = repositoryDAO.getNode(id);
                cacheManager.putExisting(NODES_CACHE, id, node);
                return node;
            }
        }
        return null;
    }

    @Override
    public List<Node> getProperties(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return Collections.emptyList();
        } else if (ids.size() == 1) {
            return Collections.singletonList(getProperties(ids.iterator().next()));
        } else {
            final Set<String> all = new HashSet<>(ids);
            final List<Node> cached = new ArrayList<>(all.size());
            for (Iterator<String> iterator = all.iterator(); iterator.hasNext(); ) {
                final Node node = cacheManager.get(NODES_CACHE, iterator.next());
                if (node != null) {
                    cached.add(node);
                    iterator.remove();
                }
            }
            if (all.isEmpty()) {
                return cached;
            } else {
                final List<Node> uncached = repositoryDAO.getNodes(all);
                for (Node node : uncached) {
                    cacheManager.putExisting(NODES_CACHE, node.id, node);
                }
                return Stream.concat(cached.stream(), uncached.stream()).collect(Collectors.toList());
            }
        }
    }

    @Override
    public List<String> getVersions(String type, String propName, Serializable propValue) {
        return repositoryDAO.queryVersions(type != null ? Collections.singletonList(type) : null, propName, propValue);
    }

    @Override
    public String getPrimaryParent(String id) {
        if (id != null) {
            final Node cached = cacheManager.get(NODES_CACHE, id);
            return cached != null ? cached.parent : repositoryDAO.getPrimaryParent(id);
        }
        return null;
    }

    @Override
    public Map<String, String> getPrimaryParents(Collection<String> ids) {
        if (ids != null && !ids.isEmpty()) {
            if (ids.size() == 1) {
                final String id = ids.iterator().next();
                final String parent = getPrimaryParent(id);
                return parent != null ? Collections.singletonMap(id, parent) : Collections.emptyMap();
            }
            Map<String, String> result = null;
            final Set<String> uncached = new HashSet<>();
            for (String id : ids) {
                final Node cached = cacheManager.get(NODES_CACHE, id);
                if (cached != null) {
                    if (result == null) {
                        result = new HashMap<>();
                    }
                    final String parent = cached.parent;
                    if (parent != null) {
                        result.put(id, parent);
                    }
                } else {
                    uncached.add(id);
                }
            }
            if (result == null) {
                return repositoryDAO.getPrimaryParents(uncached);
            } else {
                result.putAll(repositoryDAO.getPrimaryParents(uncached));
                return result;
            }
        }
        return Collections.emptyMap();
    }


    @Override
    public List<Assoc> getChildAssoc(final String parent, final String assocType, String assocName) {
        return parent != null ? filterByTypeAndName(getChildAssoc(parent), assocType, assocName) : Collections.emptyList();
    }

    @Override
    public List<Assoc> getParentAssoc(final String child, final String assocType, String assocName) {
        return child != null ? filterByTypeAndName(getParentAssoc(child), assocType, assocName) : Collections.emptyList();
    }

    @Override
    public List<Assoc> getChildAssoc(Collection<String> ids, String assocType, String assocName) {
        return ids != null ? filterByTypeAndName(getChildAssoc(ids), assocType, assocName) : Collections.emptyList();
    }

    @Override
    public int countAllChildAssocs(String id) {
        if (id == null) {
            return 0;
        }
        List<Assoc> c = cacheManager.get(CHILDREN_CACHE, id);
        if (c != null) {
            return c.size();
        }
        final Integer count = repositoryDAO.countAllChildAssoc(id, null, null);
        return count != null ? count : 0;
    }

    @Override
    public List<Assoc> getAllAssocByType(String type) {
        checkParam(type, "type is not specified");
        final List<Assoc> cached = cacheManager.get(ASSOC_CACHE, type);
        if (cached == null) {
            final List<Assoc> assocs = repositoryDAO.getAllAssocByType(type);
            cacheManager.putExisting(ASSOC_CACHE, type, assocs);
            return assocs;
        }
        return cached;
    }

    @Override
    public List<Assoc> getAllAssoc(final String id) {
        List<Assoc> p = cacheManager.get(PARENTS_CACHE, id);
        List<Assoc> c = cacheManager.get(CHILDREN_CACHE, id);
        if (p == null && c == null) {
            final List<Assoc> all = repositoryDAO.getAllAssoc(id);
            cacheManager.putExisting(CHILDREN_CACHE, id, filterChildren(id, all));
            cacheManager.putExisting(PARENTS_CACHE, id, filterParents(id, all));
            return all;
        }
        if (p == null) {
            p = repositoryDAO.getParentAssoc(id);
            cacheManager.putExisting(PARENTS_CACHE, id, p);
        }
        if (c == null) {
            c = repositoryDAO.getChildAssoc(id);
            cacheManager.putExisting(CHILDREN_CACHE, id, c);
        }
        return Stream.concat(c.stream(), p.stream()).collect(Collectors.toList());
    }

    @Override
    public void addChild(String parentId, String childId, String assocType, String assocName, boolean checkIfExists) {
        checkParam(parentId, "parent node is null");
        checkParam(childId, "child node is null");
        checkParam(assocType, "assoc type is null");
        if (!checkIfExists || !(repositoryDAO.countAllChildAssoc(parentId, childId, assocType) > 0)) {
            repositoryDAO.createAssoc(parentId, childId, assocType, assocName);
            cacheManager.markAsCreated(CHILDREN_CACHE, parentId);
            cacheManager.markAsCreated(PARENTS_CACHE, childId);
            cacheManager.markAsCreated(ASSOC_CACHE, assocType);
        }
    }

    @Override
    public void deleteNode(String id) {
        checkParam(id, "node id is null");
        deleteNode(id, repositoryDAO.getPrimaryParent(id));
        cacheManager.clear(ASSOC_CACHE);
    }

    @Override
    public void deleteChild(String parentId, String childId, String assocType, String assocName) {
        checkParam(parentId, "parent node id is null");
        checkParam(childId, "child node id is null");
        repositoryDAO.deleteAssoc(parentId, childId, assocType, assocName);
        final String primaryParent = repositoryDAO.getPrimaryParent(childId);
        if (Objects.equals(parentId, primaryParent) && 0 == repositoryDAO.countAllChildAssoc(parentId, childId, null)) {
            deleteNode(childId, primaryParent);
        }
        cacheManager.remove(PARENTS_CACHE, childId);
        cacheManager.remove(ASSOC_CACHE, assocType);
        cacheManager.remove(CHILDREN_CACHE, parentId);
    }

    @Override
    public void move(final String id, String to, String assocType, String assocName) {
        checkParam(id, "node id is null");
        final String from = getPrimaryParent(id);
        repositoryDAO.setPrimaryParent(id, to);
        if (assocType != null) {
            repositoryDAO.createAssoc(to, id, assocType, assocName);
            repositoryDAO.deleteAssoc(from, id, assocType, assocName);
        }
        cacheManager.remove(NODES_CACHE, id);
        cacheManager.remove(CHILDREN_CACHE, from);
        cacheManager.remove(CHILDREN_CACHE, to);
        cacheManager.remove(PARENTS_CACHE, id);
        cacheManager.remove(ASSOC_CACHE, assocType);
    }

    @Override
    public boolean exists(String id) {
        return GUID.is(id) && (cacheManager.contains(NODES_CACHE, id) || repositoryDAO.exists(id));
    }

    @Override
    public List<String> search(Collection<String> types, String propName, Serializable propValue) {
        return repositoryDAO.query(types, propName, propValue);
    }

    @Override
    public List<String> search(QueryBuilder q) {
        return search(q, null, -1);
    }

    @Override
    public List<String> search(QueryBuilder q, Collection<String> primaryParents, int limit) {
        checkParam(q, "query builder is null");
        return repositoryDAO.queryByParents(q.build(), primaryParents, limit);
    }

    @Override
    public int count(QueryBuilder q) {
        checkParam(q, "query builder is null");
        return repositoryDAO.queryCount(q.build());
    }

    private boolean exists(Collection<String> ids) {
        for (String id : ids) {
            if(GUID.is(id) && (cacheManager.contains(NODES_CACHE, id))) {
                return true;
            }
        }
        return repositoryDAO.exists(ids);
    }

    private static void checkParam(Object id, String msg) {
        if (id == null) {
            throw new IllegalArgumentException(msg);
        }
    }

    private void checkNodeId(Node node) {
        if (node.id != null && !GUID.is(node.id)) {
            throw new IllegalArgumentException("node id must be GUID: " + node.id);
        }
     }

    private void deleteNode(String id, String parentId) {
        deleteChildren(id);
        cacheManager.remove(NODES_CACHE, id);
        cacheManager.remove(CHILDREN_CACHE, id);
        cacheManager.remove(PARENTS_CACHE, id);
        cacheManager.remove(CHILDREN_CACHE, parentId);
        repositoryDAO.deleteNode(id);
        if (fullTextIndexer != null) {
            fullTextIndexer.remove(id);
        }
    }

    private void deleteChildren(String id) {
        final List<String> children = repositoryDAO.getByPrimaryParent(id);
        if (children != null) {
            for (String child : children) {
                cacheManager.remove(NODES_CACHE, child);
                cacheManager.remove(CHILDREN_CACHE, child);
                cacheManager.remove(PARENTS_CACHE, child);
                deleteChildren(child);
                if (fullTextIndexer != null) {
                    fullTextIndexer.remove(child);
                }
            }
            cacheManager.remove(CHILDREN_CACHE, id);
            repositoryDAO.deleteNodes(children);
        }
        repositoryDAO.deleteAllAssoc(id);
    }

    private static List<Assoc> filterParents(final String id, List<Assoc> all) {
        return all.stream().filter(a -> a != null && Objects.equals(a.target, id)).collect(Collectors.toList());
    }

    private static List<Assoc> filterChildren(final String id, List<Assoc> all) {
        return all.stream().filter(a -> a != null && Objects.equals(a.source, id)).collect(Collectors.toList());
    }

    private List<Assoc> filterByTypeAndName(List<Assoc> assocs, final String assocType, final String assocName) {
        return assocType != null || assocName != null
                ? assocs.stream().filter(input ->
                input != null && (assocType == null || assocType.equals(input.type)) && (assocName == null || assocName.equals(input.name))).
                collect(Collectors.toList())
                : assocs;
    }

    private List<Assoc> getChildAssoc(String id) {
        final List<Assoc> result = cacheManager.get(CHILDREN_CACHE, id);
        if (result == null) {
            final List<Assoc> assocs = repositoryDAO.getChildAssoc(id);
            cacheManager.putExisting(CHILDREN_CACHE, id, assocs);
            return assocs;
        }
        return result;
    }

    private List<Assoc> getParentAssoc(String id) {
        final List<Assoc> result = cacheManager.get(PARENTS_CACHE, id);
        if (result == null) {
            final List<Assoc> assocs = repositoryDAO.getParentAssoc(id);
            cacheManager.putExisting(PARENTS_CACHE, id, assocs);
            return assocs;
        }
        return result;
    }

    private List<Assoc> getChildAssoc(Collection<String> ids) {
        if (ids != null) {
            if (ids.size() == 1) {
                return getChildAssoc(ids.iterator().next());
            }
            final List<Assoc> result = new ArrayList<>();
            final List<String> uncached = new ArrayList<>(ids.size());
            for (String id : ids) {
                final Collection<Assoc> cached = cacheManager.get(CHILDREN_CACHE, id);
                if (cached != null) {
                    result.addAll(cached);
                } else {
                    uncached.add(id);
                }
            }
            if (!uncached.isEmpty()) {
                final List<Assoc> assocs = repositoryDAO.getChildAssoc(uncached);
                if (!assocs.isEmpty()) {
                    final Map<String, Collection<Assoc>> assocByParent = new HashMap<>();
                    for (Assoc assoc : assocs) {
                        assocByParent.computeIfAbsent(assoc.source, k -> new ArrayList<>()).add(assoc);
                    }
                    for (Map.Entry<String, Collection<Assoc>> entry : assocByParent.entrySet()) {
                        cacheManager.putExisting(CHILDREN_CACHE, entry.getKey(), entry.getValue());
                    }
                }
                result.addAll(assocs);
            }
            return result;
        } else {
            return Collections.emptyList();
        }
    }

    @SuppressWarnings("unused")
    public void clear() {
        repositoryDAO.clear();
    }
}
