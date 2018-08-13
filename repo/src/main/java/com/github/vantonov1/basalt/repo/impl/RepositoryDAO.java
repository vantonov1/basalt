package com.github.vantonov1.basalt.repo.impl;

import com.github.vantonov1.basalt.repo.AbstractJdbcDAO;
import com.github.vantonov1.basalt.repo.Assoc;
import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.Pair;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;
import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class RepositoryDAO extends AbstractJdbcDAO {
    private final ResultSetExtractor<String> GET_PARENT_ID = rs -> rs.next() ? rs.getString("parent_id") : null;
    private final ResultSetExtractor<Date> GET_MODIFIED = rs -> rs.next() ? new Date(rs.getLong("modified")) : null;
    private final ResultSetExtractor<Integer> GET_COUNT = rs -> rs.next() ? rs.getInt(1) : 0;

    private final ResultSetExtractor<List<String>> GET_IDS = rs -> {
        final List<String> result = new ArrayList<>();
        int index = 0;
        setFetchSize(rs, index);
        while (rs.next()) {
            final String id = rs.getString("id");
            if (id != null) {
                result.add(id);
            }
            setFetchSize(rs, index++);
        }
        return result;
    };

    private final ResultSetExtractor<List<Pair<String, String>>> GET_PARENT_IDS = rs -> {
        final List<Pair<String, String>> result = new ArrayList<>();
        int index = 0;
        setFetchSize(rs, index);
        while (rs.next()) {
            final String parent_id = rs.getString("parent_id");
            final String id = rs.getString("id");
            assert id != null;
            if (parent_id != null) {
                result.add(new Pair<>(id, parent_id));
            }
            setFetchSize(rs, index++);
        }
        return result;
    };

    private final ResultSetExtractor<Serializable> GET_PROP = rs -> {
        Serializable result = null;
        int index = 0;
        setFetchSize(rs, index);
        while (rs.next()) {
            final int type = rs.getShort("type");
            final Serializable value = getValue(rs);
            if (type < 0) {// Multi
                if (result == null) {
                    result = new ArrayList<>();
                }
                assert result instanceof List;
                ((List) result).add(value);
            } else if (value != null) {
                assert result == null;
                result = value;
            }
            setFetchSize(rs, index++);
        }
        return result;
    };

    private final ResultSetExtractor<Map<String, Object>> GET_PROP_BY_IDS = rs -> {
        final Map<String, Object> result = new HashMap<>();
        int index = 0;
        setFetchSize(rs, index);
        while (rs.next()) {
            final int type = rs.getShort("type");
            final String node_id = rs.getString("node_id");
            final Serializable value = getValue(rs);
            if (type < 0) {// Multi
                Object r = result.computeIfAbsent(node_id, k -> new ArrayList<>());
                assert r instanceof List;
                ((List) r).add(value);
            } else {
                result.put(node_id, value);
            }
            setFetchSize(rs, index++);
        }
        return result;
    };

    private final ResultSetExtractor<Node> GET_NODE = rs -> {
        final Node result = new Node();
        String clazz = null;
        String id = null;
        String parent_id = null;
        Long modified = null;
        String version = null;
        int index = 0;
        setFetchSize(rs, index);
        while (rs.next()) {
            clazz = rs.getString("class");
            id = rs.getString("id");
            parent_id = rs.getString("parent_id");
            modified = rs.getLong("modified");
            version = rs.getString("version");
            assert !id.equals(parent_id);
            final int type = rs.getShort("type");
            final Serializable value = getValue(rs);
            final String name = rs.getString("name");
            putValue(result, name, value, type);
            setFetchSize(rs, index++);
        }
        if (clazz != null) {
            result.type = clazz;
            result.id = id;
            result.parent = parent_id;
            result.modified = new Date(modified);
            result.version = "T".equals(version);
            return result;
        } else {
            return null;
        }
    };

    private final ResultSetExtractor<List<Node>> GET_NODES = rs -> {
        final Map<String, Node> result = new HashMap<>();
        int index = 0;
        setFetchSize(rs, index);
        while (rs.next()) {
            extractNode(rs, result);
            setFetchSize(rs, index++);
        }
        return new ArrayList<>(result.values());
    };

    private final ResultSetExtractor<List<Assoc>> GET_ASSOCS = rs -> {
        final List<Assoc> result = new ArrayList<>();
        int index = 0;
        setFetchSize(rs, index);
        while (rs.next()) {
            final String type = rs.getString("type");
            final String name = rs.getString("name");
            assert type != null;
            result.add(new Assoc(type, name, rs.getString("source"), rs.getString("target")));
            setFetchSize(rs, index++);
        }
        return result;
    };

    //    private final ResultSetExtractor<Map<String, String>> GET_RELATED_BY_PROPS = rs -> {
//        final Map<String, String> result = new HashMap<>();
//        int index = 0;
//        setFetchSize(rs, index);
//        while (rs.next()) {
//            final String to_id = rs.getString("to_id");
//            final String from_id = rs.getString("from_id");
//            result.put(from_id, to_id);
//            setFetchSize(rs, index++);
//        }
//        return result;
//    };

//    private final ResultSetExtractor<List<Node>> GET_ASSOC_NODES = new ResultSetExtractor<List<Node>>() {
//        @Override
//        public List<Node> extractData(ResultSet rs) throws SQLException, DataAccessException {
//            final Map<String, Node> result = new HashMap<>();
//            int index = 0;
//            setFetchSize(rs, index);
//            while (rs.next()) {
//                final Node node = extractNode(rs, result);
//                final String assocType = rs.getString("assocType");
//                final String assocName = rs.getString("assocName");
//                node.add(BaseAlfrescoTypes.PROP_RELATION_TYPE, deserializeQName(assocType));
//                node.add(BaseAlfrescoTypes.PROP_RELATION_NAME, deserializeQName(assocName));
//                setFetchSize(rs, index++);
//            }
//            return Lists.newArrayList(result.values());
//        }
//    };

    public RepositoryDAO(DataSource dataSource) {
        super(dataSource);
    }

    public String createNode(Node node, String parentId) {
        final String id = node.id != null ? node.id : GUID.generate();
        update("insert into bst_node (id, modified, parent_id, class, version) values (?, ?, ?, ?, ?)", id, System.currentTimeMillis(), parentId, node.type, Boolean.TRUE.equals(node.version) ? "T" : null);
        insertProperties(id, node);

        return id;
    }

    public List<String> createNodes(Collection<Node> nodes, String parentId) {
        final List<Object[]> nodesBatch = new ArrayList<>();
        final List<Object[]> propertiesBatch = new ArrayList<>();
        final List<String> result = nodes.stream().map(node -> fillBatch(parentId, node, nodesBatch, propertiesBatch)).collect(Collectors.toList());
        insertNodes(nodesBatch);
        insertProperties(propertiesBatch);
        return result;
    }

    public Map<String, Collection<String>> createNodes(Map<String, Collection<Node>> nodes) {
        final List<Object[]> nodesBatch = new ArrayList<>();
        final List<Object[]> propertiesBatch = new ArrayList<>();
        final Map<String, Collection<String>> result = new HashMap<>();
        for (Map.Entry<String, Collection<Node>> entry : nodes.entrySet()) {
            final String parentId = entry.getKey();
            final List<String> ids = entry.getValue().stream().map((node -> fillBatch(parentId, node, nodesBatch, propertiesBatch))).collect(Collectors.toList());
            result.put(parentId, ids);
        }
        insertNodes(nodesBatch);
        insertProperties(propertiesBatch);
        return result;
    }

    private String fillBatch(String parentId, Node node, List<Object[]> nodesBatch, List<Object[]> propertiesBatch) {
        final String id = node.id != null ? node.id : GUID.generate();
        nodesBatch.add(new Object[]{id, System.currentTimeMillis(), parentId, (node.type)});
        if (node.hasProperties()) {
            for (Map.Entry<String, Serializable> entry : node.getProperties().entrySet()) {
                insertProperty(id, propertiesBatch, (entry.getKey()), entry.getValue());
            }
        }
        return id;
    }

    public boolean exists(String id) {
        return id != null && query("select count(id) from bst_node where id=?", GET_COUNT, id) > 0;
    }

    public boolean exists(Collection<String> ids) {
        return !ids.isEmpty() && new Query("select count(id) from bst_node").where("id", ids).run(GET_COUNT) > 0;
    }

    public Node getNode(String id) {
        return query("select * from bst_node n left join bst_props p on p.node_id = n.id where n.id=?", GET_NODE, id);
    }

    public List<Node> getNodes(Collection<String> ids) {
        return queryBulk("select * from bst_node n left join bst_props p on n.id = p.node_id", "n.id", ids, -1, GET_NODES);
    }

    public void updateNode(String id, Node node, boolean deleteOld) {
        final Node old = getNode(id);
        if (old == null) {
            throw new IllegalArgumentException("node not found: " + id);
        }
        if (Boolean.TRUE.equals(old.version)) {
            throw new IllegalArgumentException("version node could not be updated: " + id);
        }
        if (node.type != null && !node.type.equals(old.type)) {
            update("update bst_node set class = ? where id = ?", (node.type), id);
        }
        if (old.hasProperties()) {
            final List<Object[]> updateBatch = new ArrayList<>();
            final List<Object[]> insertBatch = new ArrayList<>();
            final List<Object[]> deleteBatch = new ArrayList<>();
            final List<Object[]> deleteValuesBatch = new ArrayList<>();
            updateProperties(id, node, old, updateBatch, insertBatch, deleteBatch, deleteValuesBatch);
            if (deleteOld && old.hasProperties()) {
                for (String name : old.getProperties().keySet()) {
                    deleteBatch.add(new Object[]{id, (name)});
                }
            }
            deleteValues(deleteValuesBatch);
            deleteProperties(deleteBatch);
            updateProperties(updateBatch);
            insertProperties(insertBatch);
            setModified(id, old.modified, updateBatch, insertBatch, deleteBatch, deleteValuesBatch);
        } else {
            insertProperties(id, node);
            setModified(id, old.modified);
        }
    }

    public void updateProperties(Collection<Node> nodes, Collection<Node> oldNodes, boolean deleteOld) {
        final List<Object[]> updateBatch = new ArrayList<>();
        final List<Object[]> insertBatch = new ArrayList<>();
        final List<Object[]> deleteBatch = new ArrayList<>();
        final List<Object[]> deleteValuesBatch = new ArrayList<>();
        final List<String> ids = new ArrayList<>(nodes.size());
        for (Node node : nodes) {
            final String id = node.id;
            ids.add(id);
            final Optional<Node> old = oldNodes.stream().filter((Node n) -> n.id.equals(id)).findFirst();
            if (!old.isPresent()) {
                throw new IllegalArgumentException("node not found " + id);
            }
            final Node oldNode = old.get();
            if (Boolean.TRUE.equals(oldNode.version)) {
                throw new IllegalArgumentException("version node could not be updated: " + id);
            }
            if (oldNode.hasProperties()) {
                updateProperties(id, node, oldNode, updateBatch, insertBatch, deleteBatch, deleteValuesBatch);
                if (deleteOld && oldNode.hasProperties()) {
                    for (String name : oldNode.getProperties().keySet()) {
                        deleteBatch.add(new Object[]{id, (name)});
                    }
                }
            } else if (node.hasProperties()) {
                for (Map.Entry<String, Serializable> entry : node.getProperties().entrySet()) {
                    insertProperty(id, insertBatch, (entry.getKey()), entry.getValue());
                }
            }
        }
        updateProperties(updateBatch);
        insertProperties(insertBatch);
        deleteValues(deleteValuesBatch);
        deleteProperties(deleteBatch);
        setModified(ids, updateBatch, insertBatch, deleteBatch, deleteValuesBatch);
    }

    public void deleteNode(String id) {
        update("delete from bst_node where id = ? and version is null", id);
    }

    public void deleteNodes(Collection<String> ids) {
        if (ids != null && !ids.isEmpty()) {
            new Query("delete from bst_node").where("id", ids).noVersions().update();
        }
    }

    public String getPrimaryParent(String id) {
        return query("select parent_id from bst_node where id = ?", GET_PARENT_ID, id);
    }

    public List<String> getByPrimaryParent(String id) {
        return query("select id from bst_node where parent_id = ?", GET_IDS, id);
    }

    public Map<String, String> getPrimaryParents(Collection<String> ids) {
        final List<Pair<String, String>> parents = queryBulk("select id, parent_id from bst_node", "id", ids, -1, GET_PARENT_IDS);
        if (parents.isEmpty()) {
            return Collections.emptyMap();
        }
        final Map<String, String> result = new HashMap<>();
        for (Pair<String, String> p : parents) {
            result.put(p.getFirst(), p.getSecond());
        }
        return result;
    }

    public void setPrimaryParent(String id, String parentId) {
        update("update bst_node set parent_id = ? where id = ?", parentId, id);
    }

    public <T> T getProperty(String id, String name) {
        return "__modified".equals(name)
                ? (T) query("select modified from bst_node where id = ?", GET_MODIFIED, id)
                : (T) new Query("select type, value_s, value_n from bst_props p").where("node_id", id).and("name", (name)).run(GET_PROP);
    }

    public Map<String, Object> getProperty(Collection<String> ids, String name) {
        return new Query("select * from bst_props p").where("node_id", ids).and("name", (name)).run(GET_PROP_BY_IDS);
    }

    public void setProperty(String id, String name, Date modified, Object value, Object oldValue) {
        if (value instanceof Collection || oldValue instanceof Collection) {
            final List<Object[]> insertBatch = new ArrayList<>();
            final List<Object[]> updateBatch = new ArrayList<>();
            final List<Object[]> deleteValuesBatch = new ArrayList<>();
            updateProperty(id, (name), value, oldValue, insertBatch, updateBatch, deleteValuesBatch);
            deleteValues(deleteValuesBatch);
            updateProperties(updateBatch);
            insertProperties(insertBatch);
            setModified(id, modified, updateBatch, insertBatch, deleteValuesBatch);
        } else {
            if (oldValue != null) {
                updateProperty(id, name, value);
            } else {
                insertProperty(id, name, value);
            }
            setModified(id, modified);
        }
    }

    public void setProperty(Collection<String> ids, String name, Object value, Map<String, Object> oldValue) {
        final List<Object[]> insertBatch = new ArrayList<>();
        final List<Object[]> updateBatch = new ArrayList<>();
        final List<Object[]> deleteValuesBatch = new ArrayList<>();
        if (value instanceof Collection) {
            for (String id : ids) {
                final Object existing = oldValue != null ? oldValue.get(id) : null;
                updateProperty(id, name, value, existing, insertBatch, updateBatch, deleteValuesBatch);
            }
            deleteValues(deleteValuesBatch);
        } else {
            for (String id : ids) {
                final Object existing = oldValue != null ? oldValue.get(id) : null;
                if (existing != null) {
                    if (!Objects.equals(value, existing)) {
                        updateProperty(id, updateBatch, name, value);
                    }
                } else {
                    insertProperty(id, insertBatch, name, value);
                }
            }
        }
        updateProperties(updateBatch);
        insertProperties(insertBatch);
        setModified(ids, updateBatch, insertBatch, deleteValuesBatch);
    }

    public void removeProperty(String id, String name, Date modified) {
        update("delete from bst_props where node_id = ? and name = ?", id, (name));
        setModified(id, modified);
    }

    public void removeProperty(Collection<String> ids, String name) {
        new Query("delete from bst_props").where("node_id", ids).and("name", name).update();
        setModified(ids);
    }

    public void createAssoc(String source, String target, String assocType, String assocName) {
        if (assocName != null) {
            update("insert into bst_assoc (type, name, source, target) values (?, ?, ?, ?)", (assocType), (assocName), source, target);
        } else {
            update("insert into bst_assoc (type, source, target) values (?, ?, ?)", (assocType), source, target);

        }
    }

    public void createAssocs(String source, Collection<String> targets, String assocType) {
        final List<Object[]> batch = new ArrayList<>();
        for (String id : targets) {
            batch.add(new Object[]{assocType, source, id});
        }
        insertAssoc(batch);
    }

    public List<Assoc> getChildAssoc(String id) {
        return new Query("select * from bst_assoc a").where("a.source", id).run(GET_ASSOCS);
    }

    public List<Assoc> getParentAssoc(String id) {
        return new Query("select * from bst_assoc").where("target", id).run(GET_ASSOCS);
    }

    public List<Assoc> getAllAssoc(String id) {
        return new Query("select * from bst_assoc a").where(new String[]{"a.source", "a.target"}, id).run(GET_ASSOCS);
    }

    public List<Assoc> getAllAssocByType(String type) {
        return new Query("select * from bst_assoc").where("type", type).run(GET_ASSOCS);
    }


    public List<Assoc> getChildAssoc(List<String> ids) {
        return queryBulk("select * from bst_assoc", "bst_assoc.source", ids, -1, GET_ASSOCS);
    }

    public Integer countAllChildAssoc(String parentId, String childId, String assocType) {
        return new Query("select count(*) from bst_assoc")
                .where("type", assocType)
                .and("source", parentId)
                .and("target", childId)
                .run(GET_COUNT);
    }

    public void deleteAssoc(String source, String target, String assocType, String assocName) {
        new Query("delete from bst_assoc")
                .where("type", assocType)
                .and("source", source)
                .and("target", target)
                .and("name", assocName)
                .update();
    }

    public void deleteAllAssoc(String id) {
        update("delete from bst_assoc where source = ? or target = ?", id, id);
    }

    //    public List<Node> getChildProperties(String id, final String type, String assocName) {
//        final List<Node> result = new Query("select n.*, p.name, p.type, p.value_s, p.value_n, a.type as assocType, a.name as assocName from bst_assoc a join bst_node n on n.id = a.target join bst_props p on p.node_id = a.target")
//                .where("a.source", id)
//                .and("a.type", type)
//                .and("a.name", assocName)
//                .noVersions()
//                .run(GET_ASSOC_NODES);
//        for (Node node : result) {
//            node.put(BaseAlfrescoTypes.PROP_RELATION_DIRECTION, "target");
//        }
//        return result;
//    }
//
//    public List<Node> getParentProperties(String id, final String type) {
//        final List<Node> result = new Query("select n.*, p.*, a.type as assocType, a.name as assocName from bst_assoc a join bst_node n on n.id = a.source join bst_props p on p.node_id = a.source")
//                .where("a.target", id)
//                .and("a.type", type)
//                .noVersions()
//                .run(GET_ASSOC_NODES);
//        for (Node node : result) {
//            node.put(BaseAlfrescoTypes.PROP_RELATION_DIRECTION, "source");
//        }
//        return result;
//    }

//    public List<String> query(String sql, Collection<String> ids, int maxRows) {
//        return queryBulk(sql, "n.id", ids, maxRows, GET_IDS);
//    }

    public List<String> queryByParents(String sql, Collection<String> ids, int maxRows) {
        return new Query("select distinct n.id from bst_node n " + sql).noVersions().setMaxRows(maxRows).run("n.parent_id", ids, GET_IDS);
    }

    public int queryCount(String sql) {
        return new Query("select count(distinct n.id) from bst_node n " + sql).noVersions().run(GET_COUNT);
    }

    public List<String> query(Collection<String> types, String propName, Serializable propValue) {
        return createQuery(types, propName, propValue).noVersions().run(GET_IDS);
    }

    public List<String> queryVersions(Collection<String> types, String propName, Serializable propValue) {
        return createQuery(types, propName, propValue).versions().run(GET_IDS);
    }

    private Query createQuery(Collection<String> types, String propName, Serializable propValue) {
        final String v = TypeConverter.getString(propValue);
        final Long n = TypeConverter.getNumeric(propValue);
        return new Query("select n.id from bst_node n left join bst_props p on p.node_id = n.id")
                .where("n.class", types)
                .and("p.name", propName)
                .and("p.value_s", v)
                .and("p.value_n", n);
    }

    private void extractNode(ResultSet rs, Map<String, Node> result) throws SQLException {
        final String id = rs.getString("id");
        final String parent_id = rs.getString("parent_id");
        assert !id.equals(parent_id);
        final int type = rs.getShort("type");
        final Serializable value = getValue(rs);
        final String name = rs.getString("name");
        final long modified = rs.getLong("modified");
        final String version = rs.getString("version");
        Node node = result.get(id);
        if (node == null) {
            node = new Node();
            node.type = rs.getString("class");
            node.parent = parent_id;
            node.modified = new Date(modified);
            node.version = "T".equals(version);
            node.id = id;
            result.put(id, node);
        }
        putValue(node, name, value, type);
    }

    private static Serializable getValue(ResultSet rs) throws SQLException {
        return TypeConverter.convert(rs.getShort("type"), rs.getString("value_s"), rs.getLong("value_n"));
    }

    private void putValue(Node node, String name, Serializable value, int type) {
        if (value != null && name != null) {
            final Serializable existing = node.get(name);
            if (existing != null) {
                assert type < 0 && existing instanceof List : "dup non-list value " + name + " " + value;
                ((List) existing).add(value);
            } else if (type < 0) {
                final List<Object> l = new ArrayList<>();
                l.add(value);
                node.put(name, (Serializable) l);
            } else {
                node.put(name, value);
            }
        }
    }

    private void updateProperties(String id, Node node, Node old, List<Object[]> updateBatch, List<Object[]> insertBatch, List<Object[]> deleteBatch, List<Object[]> deleteValuesBatch) {
        for (Map.Entry<String, Serializable> updated : node.getProperties().entrySet()) {
            final String name = updated.getKey();
            final Serializable value = updated.getValue();
            if (value != null) {
                final Serializable oldValue = old.getProperties().remove(updated.getKey());
                updateProperty(id, name, value, oldValue, insertBatch, updateBatch, deleteValuesBatch);
            } else {
                deleteBatch.add(new Object[]{id, name});
            }
        }
    }

    private void updateProperty(String id, String name, Object value, Object oldValue, List<Object[]> insertBatch, List<Object[]> updateBatch, List<Object[]> deleteValuesBatch) {
        assert value != null;
        if (!Objects.equals(oldValue, value)) {
            if (oldValue == null) {
                insertProperty(id, insertBatch, name, value);
            } else {
                if (value instanceof Collection) {
                    if (oldValue instanceof Collection) {
                        final List<Object> oldValues = new ArrayList<>(((Collection<?>) oldValue));
                        for (Object o : ((Collection) value)) {
                            if (!oldValues.remove(o)) {
                                insertProperty(id, insertBatch, name, o, true);
                            }
                        }
                        for (Object o : oldValues) {
                            deleteValue(id, deleteValuesBatch, name, o);
                        }
                    } else {
                        deleteValue(id, deleteValuesBatch, name, oldValue);
                        for (Object o : ((Collection) value)) {
                            insertProperty(id, insertBatch, name, o, true);
                        }
                    }
                } else if (oldValue instanceof Collection) {
                    for (Object o : ((Collection) oldValue)) {
                        deleteValue(id, deleteValuesBatch, name, o);
                    }
                    insertProperty(id, insertBatch, name, value);
                } else {
                    updateProperty(id, updateBatch, name, value);
                }
            }
        }
    }

    private static void deleteValue(String id, List<Object[]> deleteValuesBatch, String name, Object oldValue) {
        final String v = TypeConverter.getString(oldValue);
        final Long n = TypeConverter.getNumeric(oldValue);
        deleteValuesBatch.add(new Object[]{id, name, v, n});
    }

    private void updateProperty(String id, String name, Object value) {
        final String v = TypeConverter.getString(value);
        if (v != null) {
            update("update bst_props set value_s = ? where node_id = ? and name = ?", v, id, (name));
        } else {
            final Long n = TypeConverter.getNumeric(value);
            assert n != null;
            update("update bst_props set value_n = ? where node_id = ? and name = ?", n, id, (name));
        }
    }

    private void updateProperty(String id, List<Object[]> batch, String String, Object value) {
        final String v = TypeConverter.getString(value);
        final Long n = TypeConverter.getNumeric(value);
        batch.add(new Object[]{v != null ? v : TypeConverter.NULL_STRING, n != null ? n : TypeConverter.NULL_NUMERIC, id, String});
    }

    private void insertProperty(String id, String name, Object value) {
        final int type = TypeConverter.getType(value);
        final String v = TypeConverter.getString(value);
        if (v != null) {
            update("insert into bst_props (node_id, name, type, value_s) values (?, ?, ?, ?)", id, (name), type, v);
        } else {
            final Long n = TypeConverter.getNumeric(value);
            assert n != null;
            update("insert into bst_props (node_id, name, type, value_n) values (?, ?, ?, ?)", id, (name), type, n);
        }
    }

    private void insertProperty(String id, List<Object[]> batch, String name, Object value) {
        insertProperty(id, batch, name, value, false);
    }

    private void insertProperty(String id, List<Object[]> batch, String name, Object value, boolean multi) {
        if (value instanceof Collection) {
            for (Object o : ((Collection) value)) {
                insertProperty(id, batch, name, o, true);
            }
        } else {
            final int type = TypeConverter.getType(value);
            final String v = TypeConverter.getString(value);
            final Long n = TypeConverter.getNumeric(value);
            batch.add(new Object[]{id, name, type * (multi ? -1 : 1), v != null ? v : TypeConverter.NULL_STRING, n != null ? n : TypeConverter.NULL_NUMERIC});
        }
    }

    private void insertNodes(List<Object[]> batch) {
        batchUpdate("insert into bst_node (id, modified, parent_id, class) values (?, ?, ?, ?)", batch, 0);
    }

    private void insertProperties(String id, Node node) {
        if (node.hasProperties()) {
            final List<Object[]> insertBatch = new ArrayList<>();
            for (Map.Entry<String, Serializable> entry : node.getProperties().entrySet()) {
                insertProperty(id, insertBatch, (entry.getKey()), entry.getValue());
            }
            insertProperties(insertBatch);
        }
    }

    private void insertProperties(List<Object[]> batch) {
        batchUpdate("insert into bst_props (node_id, name, type, value_s, value_n) values (?, ?, ?, ?, ?)", batch, 0);
    }

    private void updateProperties(List<Object[]> batch) {
        batchUpdate("update bst_props set value_s = ?, value_n = ? where node_id = ? and name = ?", batch, 2);
    }

    private void deleteProperties(List<Object[]> batch) {
        batchUpdate("delete from bst_props where node_id = ? and name = ?", batch, 0);
    }

    private void deleteValues(List<Object[]> batch) {
        batchUpdate("delete from bst_props where node_id = ? and name = ? and (value_s = ? or value_n = ?)", batch, 0);
    }

    private void insertAssoc(List<Object[]> batch) {
        batchUpdate("insert into bst_assoc (type, source, target) values (?, ?, ?)", batch);
    }

    public void clear() {
        update("delete from bst_aces");
        update("delete from bst_assoc");
        update("delete from bst_props");
        update("delete from bst_node");
    }

    private void setModified(String id, Date prev) {
        if (prev == null) {
            update("update bst_node set modified = ? where id = ?", System.currentTimeMillis(), id);
        } else {
            final int affected = update("update bst_node set modified = ? where id = ? and modified = ?", System.currentTimeMillis(), id, prev.getTime());
            if (affected != 1) {
                throw new OptimisticLockingFailureException("already modified");
            }
        }
    }

    @SafeVarargs
    private final void setModified(String id, Date prev, final List<Object[]>... batches) {
        if (batches != null) {
            for (List<Object[]> objects : batches) {
                if (!objects.isEmpty()) {
                    setModified(id, prev);
                    return;
                }
            }
        }
    }

    @SafeVarargs
    private final void setModified(Collection<String> ids, final List<Object[]>... batches) {
        if (batches != null) {
            for (List<Object[]> batch : batches) {
                if (!batch.isEmpty()) {
                    setModified(ids);
                    return;
                }
            }
        }
    }

    private void setModified(Collection<String> ids) {
        new Query("update bst_node").set("modified", System.currentTimeMillis()).where("id", ids).update();
    }
}
