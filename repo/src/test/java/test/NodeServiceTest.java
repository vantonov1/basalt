package test;

import com.github.vantonov1.basalt.repo.Assoc;
import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.NodeService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.transaction.PlatformTransactionManager;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class NodeServiceTest extends BaseTest {
    @Autowired
    private NodeService nodeService;

    @Autowired
    PlatformTransactionManager transactionManager;

    @Test
    public void testNodeCRUD() throws SQLException {
        final Object tx = beginTx(false);
        final Node node = new Node("testNode", Collections.singletonMap("title", "abc"));

        final String id = nodeService.createNode(null, node, null, null);
        Assert.assertTrue(nodeService.exists(id));
        Assert.assertNotNull(id);
        final Node created = nodeService.getProperties(id);
        Assert.assertNotNull(created);
        Assert.assertEquals("testNode", created.type);
        Assert.assertEquals("abc", created.get("title"));

        Node updated = new Node(id, "testNode", Collections.<String, Serializable>singletonMap("title", "def"));
        nodeService.updateProperties(updated, true);
        updated = nodeService.getProperties(id);
        Assert.assertNotNull(updated);
        Assert.assertEquals("testNode", updated.type);
        Assert.assertEquals("def", updated.get("title"));

        nodeService.deleteNode(id);
        Assert.assertFalse(nodeService.exists(id));
        final Node deleted = nodeService.getProperties(id);
        Assert.assertNull(deleted);
        commit(tx);
    }

    @Test
    public void testProperties() throws SQLException {
        final Object tx = beginTx(false);

        final Map<String, Serializable> abc = Collections.<String, Serializable>singletonMap("title", "abc");
        final String id = nodeService.createNode(null, new Node("type", abc), null, null);
        Assert.assertEquals("abc", nodeService.getProperty(id, "title"));

        nodeService.setProperty(id, "title", null);
        Assert.assertNull(nodeService.getProperty(id, "title"));

        nodeService.setProperty(id, "title", "def");
        Assert.assertEquals("def", nodeService.getProperty(id, "title"));

        final Map<String, Serializable> update = Collections.<String, Serializable>singletonMap("prop1", "1");
        nodeService.updateProperties(new Node(id, "type", update), false);
        Node updated = nodeService.getProperties(id);
        Assert.assertEquals("def", updated.get("title"));
        Assert.assertEquals("1", updated.get("prop1"));
        Assert.assertEquals(id, updated.id);

        nodeService.removeProperty(id, "prop1");
        Assert.assertNull(nodeService.getProperty(id, "prop1"));

        final Node node2 = new Node(id, "type", abc);
        nodeService.updateProperties(node2, true);
        Assert.assertEquals("abc", nodeService.getProperty(id, "title"));
        Assert.assertNull(nodeService.getProperty(id, "prop1"));

        Assert.assertEquals(1, nodeService.getProperties(Collections.singletonList(id)).size());

        final List<String> ids = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            final Node node = new Node("type", abc);
            ids.add(nodeService.createNode(null, node, null, null));
        }
        Assert.assertEquals(10, nodeService.getProperties(ids).size());
        commit(tx);
    }

    @Test
    public void testBulk() throws SQLException {
        final Object tx = beginTx(false);

        final String parent = nodeService.createNode(null, new Node("type", Collections.emptyMap()), null, null);
        final String anotherParent = nodeService.createNode(null, new Node("type", Collections.emptyMap()), null, null);
        final List<Node> nodes = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            nodes.add(new Node("type", new HashMap<>(Collections.singletonMap("title", "abc" + i))));
        }
        final List<String> ids = nodeService.createNodes(parent, nodes, "children");

        final List<Node> created = nodeService.getProperties(ids);
        Assert.assertEquals(ids.size(), created.size());
        for (Node node : created) {
            Assert.assertTrue(ids.contains(node.id));
        }

        for (int i = 0; i < 10; i++) {
            nodes.get(i).id = ids.get(i);
            nodes.get(i).put("title", "def" + i);
        }
        nodeService.updateProperties(nodes, true);

        final List<Node> updated = nodeService.getProperties(ids);
        for (Node node : updated) {
            Assert.assertTrue(node.<String>get("title").startsWith("def"));
        }

        for (int i = 0; i < 10; i++) {
            nodes.get(i).id = null;
        }
        for (int i = 0; i < 10; i++) {
            nodes.add(new Node("anotherType", Collections.singletonMap("title", "xyz" + i)));
        }
        nodeService.createNodes(anotherParent, nodes, "babies");
        Assert.assertEquals(10, nodeService.getAllAssocByType("children").size());
        Assert.assertEquals(10, nodeService.getChildAssoc(Arrays.asList(parent, anotherParent), "children", null).size());
        Assert.assertEquals(30, nodeService.getChildAssoc(Arrays.asList(parent, anotherParent), null, null).size());

        nodeService.setProperty(ids, "title", "123");
        final Map<String, Object> set = nodeService.getProperty(ids, "title");
        Assert.assertEquals(ids.size(), set.size());
        for (String id : set.keySet()) {
            Assert.assertTrue(ids.contains(id));
        }
        for (Object o : set.values()) {
            Assert.assertEquals("123", o);
        }

        nodeService.setProperty(ids, "title", null);
        final Map<String, Object> removed = nodeService.getProperty(ids, "title");
        Assert.assertEquals(0, removed.size());
        commit(tx);
    }

    @Test
    public void testArrays() throws SQLException {
        final Object tx = beginTx(false);
        final List<String> value = Arrays.asList("abc", "def");
        final Node node = new Node("type", Collections.singletonMap("value", (Serializable) value));

        final String id = nodeService.createNode(null, node, null, null);
        Assert.assertNotNull(id);
        final Node created = nodeService.getProperties(id);
        Assert.assertNotNull(created);

        {
            final Serializable v = created.get("value");
            Assert.assertNotNull(v);
            Assert.assertTrue(v instanceof Collection);
            final Iterator it = ((Collection) v).iterator();
            Assert.assertEquals("abc", it.next());
            Assert.assertEquals("def", it.next());
            Assert.assertTrue(!it.hasNext());
        }

        final List<String> v2 = Arrays.asList("abc", "123");
        final Node updateNode = new Node(id, "type", Collections.singletonMap("value", (Serializable) v2));
        nodeService.updateProperties(updateNode, true);
        {
            final Node updated = nodeService.getProperties(id);
            Assert.assertNotNull(updated);
            final Serializable v = updated.get("value");
            Assert.assertNotNull(v);
            Assert.assertTrue(v instanceof Collection);
            final Iterator it = ((Collection) v).iterator();
            Assert.assertEquals("abc", it.next());
            Assert.assertEquals("123", it.next());
            Assert.assertTrue(!it.hasNext());
        }
        final List<String> v3 = Arrays.asList("abc", "xyz", "123");
        nodeService.setProperty(id, "value", (Serializable) v3);
        {
            final Collection<String> v = nodeService.getProperty(id, "value");
            Assert.assertTrue(v.contains("abc"));
            Assert.assertTrue(v.contains("xyz"));
            Assert.assertTrue(v.contains("123"));
        }
        nodeService.setProperty(id, "value", 1);
        Assert.assertTrue(1L == nodeService.<Integer>getProperty(id, "value"));

        nodeService.setProperty(id, "value", (Serializable) v3);
        nodeService.setProperty(id, "value", null);
        Assert.assertNull(nodeService.getProperty(id, "value"));

        commit(tx);
    }


    @Test
    public void testAssoc() throws SQLException {
        final Object tx = beginTx(false);
        final String assocName = "111";
        final Node parentNode = new Node("type", Collections.<String, Serializable>singletonMap("title", "abc"));
        final Node childNode = new Node("type", Collections.<String, Serializable>singletonMap("title", "def"));
        final String parent = nodeService.createNode(null, parentNode, null, null);
        final String child = nodeService.createNode(parent, childNode, null, "assocType");
        final String anotherParent = nodeService.createNode(null, new Node("type", Collections.emptyMap()), null, null);
        Assert.assertEquals(parent, nodeService.getPrimaryParent(child));

        nodeService.addChild(parent, child, "assocType", assocName, true);
        Assert.assertEquals(1, nodeService.countAllChildAssocs(parent));
        final Assoc childAssoc = nodeService.getChildAssoc(parent, "assocType", assocName).iterator().next();
        Assert.assertTrue(childAssoc.target.equals(child));
        Assert.assertTrue(childAssoc.source.equals(parent));
        Assert.assertTrue(childAssoc.type.equals("assocType"));
        Assert.assertTrue(childAssoc.name.equals(assocName));

        final Assoc parentAssoc = nodeService.getParentAssoc(child, "assocType", assocName).iterator().next();
        Assert.assertTrue(parentAssoc.target.equals(child));
        Assert.assertTrue(parentAssoc.source.equals(parent));
        Assert.assertTrue(parentAssoc.type.equals("assocType"));
        Assert.assertTrue(parentAssoc.name.equals(assocName));

        final Assoc allAssoc = nodeService.getAllAssoc(child).iterator().next();
        Assert.assertTrue(allAssoc.target.equals(child));
        Assert.assertTrue(allAssoc.source.equals(parent));
        Assert.assertTrue(allAssoc.type.equals("assocType"));
        Assert.assertTrue(allAssoc.name.equals(assocName));

        nodeService.move(child, anotherParent, "assocType", null);
        final Assoc movedParentAssoc = nodeService.getParentAssoc(child, "assocType", null).iterator().next();
        Assert.assertEquals(anotherParent, movedParentAssoc.source);
        Assert.assertEquals(0, nodeService.getChildAssoc(parent, "assocType", null).size());
        Assert.assertEquals(1, nodeService.getChildAssoc(anotherParent, "assocType", null).size());


        nodeService.deleteChild(parent, child, "assocType", null);
        final List<Assoc> deleted = nodeService.getAllAssoc(parent);
        Assert.assertTrue(deleted.isEmpty());
        commit(tx);
    }

}
