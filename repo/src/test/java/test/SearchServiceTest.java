package test;

import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.NodeService;
import com.github.vantonov1.basalt.repo.QueryBuilder;
import com.github.vantonov1.basalt.repo.SearchService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SearchServiceTest extends BaseTest {
    @Autowired
    private SearchService searchService;

    @Autowired
    private NodeService nodeService;

    private String id;
    private String id2;
    private String id3;

    @Before
    public void before() throws SQLException {
        final Object set = beginTx(false);

        final Map<String, Serializable> abc = Collections.<String, Serializable>singletonMap("name", "abc");
        final Map<String, Serializable> def = new HashMap<String, Serializable>() {
            {
                put("name", "def");
                put("int", 1);
                put("date", new Date());
                put("double", -1.0);
            }
        };
        id = nodeService.createNode(null, new Node("content", abc), null, null);
        id2 = nodeService.createNode(null, new Node("content", def), null, null);
        id3 = nodeService.createNode(id, new Node("content", null), "child", null);
        commit(set);
    }

    @Test
    public void testByName() {
        final List<String> byProp = searchService.search(null, "name", "def");
        Assert.assertTrue(byProp.contains(id2));
        Assert.assertFalse(byProp.contains(id));

        final List<String> mustHave = searchService.search(new QueryBuilder().type(Arrays.asList("base","content")).is("name", "def"));
        Assert.assertTrue(mustHave.contains(id2));
        Assert.assertFalse(mustHave.contains(id));

        Assert.assertEquals(1, searchService.count(new QueryBuilder().type("content").is("name", "def")));

        final List<String> mustNotHave = searchService.search(new QueryBuilder().isNot("name", "def"));
        Assert.assertTrue(mustNotHave.contains(id));
        Assert.assertFalse(mustNotHave.contains(id2));

        final List<String> shouldHave = searchService.search(new QueryBuilder().oneOf().is("name", "def").is("name", "abc"));
        Assert.assertEquals(2, shouldHave.size());
    }

    @Test
    public void testNull() throws SQLException {
        final List<String> isNull = searchService.search(new QueryBuilder().isNull("name"));
        Assert.assertFalse(isNull.contains(id));
        Assert.assertFalse(isNull.contains(id2));
        Assert.assertTrue(isNull.contains(id3));

        final List<String> isNotNull = searchService.search(new QueryBuilder().isNotNull("name"));
        Assert.assertTrue(isNotNull.contains(id));
        Assert.assertTrue(isNotNull.contains(id2));
        Assert.assertFalse(isNotNull.contains(id3));
    }

    @Test
    public void testRanges() throws SQLException {
        final Date yesterday = new Date(System.currentTimeMillis() - 86400 * 1000);
        final Date tomorrow = new Date(System.currentTimeMillis() + 86400 * 1000);
        final List<String> openRight = searchService.search(new QueryBuilder().range("date", yesterday, null));
        Assert.assertTrue(openRight.contains(id2));
        Assert.assertFalse(openRight.contains(id));
        final List<String> dateClosed = searchService.search(new QueryBuilder().range("date", yesterday, tomorrow));
        Assert.assertTrue(dateClosed.contains(id2));
        Assert.assertFalse(dateClosed.contains(id));
        final List<String> intClosed = searchService.search(new QueryBuilder().range("int", 1, 10));
        Assert.assertTrue(intClosed.contains(id2));
        Assert.assertFalse(intClosed.contains(id));
        final List<String> doubleClosed = searchService.search(new QueryBuilder().range("double", -0.5, -1.5));
        Assert.assertTrue(doubleClosed.contains(id2));
        Assert.assertFalse(doubleClosed.contains(id));
    }

    @Test
    public void testAssoc() {
        final List<String> mustHaveParent = searchService.search(new QueryBuilder().parentAssoc(id));
        Assert.assertTrue(mustHaveParent.contains(id3));
        Assert.assertFalse(mustHaveParent.contains(id));
        Assert.assertFalse(mustHaveParent.contains(id2));

        final List<String> shouldHaveParent = searchService.search(new QueryBuilder().oneOf().parentAssoc(id));
        Assert.assertTrue(shouldHaveParent.contains(id3));
        Assert.assertFalse(shouldHaveParent.contains(id));
        Assert.assertFalse(shouldHaveParent.contains(id2));

        final List<String> mustHaveChildren = searchService.search(new QueryBuilder().childAssoc(id3));
        Assert.assertTrue(mustHaveChildren.contains(id));
        Assert.assertFalse(mustHaveChildren.contains(id3));
        Assert.assertFalse(mustHaveChildren.contains(id2));

        final List<String> mustNotHaveParent = searchService.search(new QueryBuilder().parentAssocNot(id));
        Assert.assertTrue(mustNotHaveParent.contains(id));
        Assert.assertTrue(mustNotHaveParent.contains(id2));
        Assert.assertFalse(mustNotHaveParent.contains(id3));

        final List<String> mustNotHaveChildren = searchService.search(new QueryBuilder().childAssocNot(id3));
        Assert.assertFalse(mustNotHaveChildren.contains(id));
        Assert.assertTrue(mustNotHaveChildren.contains(id3));
        Assert.assertTrue(mustNotHaveChildren.contains(id2));
    }

    @Test
    public void testGroups() {
        final List<String> shouldHaveGroups = searchService.search(new QueryBuilder()
                .group().is("int", 1).is("name", "def"));
        Assert.assertTrue(shouldHaveGroups.contains(id2));
        Assert.assertFalse(shouldHaveGroups.contains(id));
        Assert.assertFalse(shouldHaveGroups.contains(id3));
        final List<String> mustHaveGroups = searchService.search(new QueryBuilder()
                .oneOf()
                .group().is("name", "abc")
                .group().is("int", 1).is("name", "def"));
        Assert.assertTrue(mustHaveGroups.contains(id));
        Assert.assertTrue(mustHaveGroups.contains(id2));
        Assert.assertFalse(mustHaveGroups.contains(id3));
    }

}
