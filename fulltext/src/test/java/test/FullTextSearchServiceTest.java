package test;

import com.github.vantonov1.basalt.content.ContentService;
import com.github.vantonov1.basalt.fulltext.FullTextSearchService;
import com.github.vantonov1.basalt.fulltext.impl.AbstractFullTextIndexer;
import com.github.vantonov1.basalt.fulltext.impl.LuceneIndexer;
import com.github.vantonov1.basalt.repo.FullTextIndexer;
import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.NodeService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FullTextSearchServiceTest extends BaseTest {
    @Autowired
    private FullTextSearchService fullTextSearchService;

    @Autowired
    private NodeService nodeService;

    @Autowired
    private FullTextIndexer contentIndexer;

    @Autowired
    private ContentService contentService;

    private String id;

    @Before
    public void before() throws SQLException, URISyntaxException {
        final Object create = beginTx(false);
        final Map<String, Serializable> props = new HashMap<>();
        props.put("title", "abc def");
        props.put("v", "a");
        final Node node = new Node("content", props);
        id = nodeService.createNode(null, node, null, null);
        contentService.putContent(id, new File(new URI(getClass().getResource("/test-content.txt").toString())));
        commit(create);
        ((AbstractFullTextIndexer) contentIndexer).awaitTermination();
    }

    @Test
    public void testFullText() throws SQLException, URISyntaxException {
        List<String> byTitle = fullTextSearchService.search(Collections.singletonList("Abc"), null, 0);
        Assert.assertTrue(byTitle.contains(id));

        final List<String> substring = fullTextSearchService.search(Collections.singletonList("def"), null, 0);
        Assert.assertTrue(substring.contains(id));

        final List<String> byContent = fullTextSearchService.search(Collections.singletonList("Content"), null, 0);
        Assert.assertTrue(byContent.contains(id));

        final Object update = beginTx(false);
        final Node updated = new Node(id, "content", Collections.singletonMap("title", "xyz"));
        nodeService.updateProperties(updated, false);
        commit(update);
        ((AbstractFullTextIndexer) contentIndexer).awaitTermination();

        byTitle = fullTextSearchService.search(Collections.singletonList("xyz"), null, 0);
        Assert.assertTrue(byTitle.contains(id));

        final Object delete = beginTx(false);
        nodeService.deleteNode(id);
        commit(delete);
        ((AbstractFullTextIndexer) contentIndexer).awaitTermination();

        byTitle = fullTextSearchService.search(Collections.singletonList("xyz"), null, 0);
        Assert.assertFalse(byTitle.contains(id));
    }

    @Test
    public void testReindex() throws SQLException, URISyntaxException {
        final Object reindex = beginTx(false);
//        Assert.assertTrue(luceneDAO.check());
        if (contentIndexer instanceof LuceneIndexer) {
            ((LuceneIndexer) contentIndexer).reindex();
        }
        commit(reindex);

        final List<String> byTitle = fullTextSearchService.search(Collections.singletonList("Abc"), null, 0);
        Assert.assertTrue(byTitle.contains(id));

        final List<String> byContent = fullTextSearchService.search(Collections.singletonList("Content"), null, 0);
        Assert.assertTrue(byContent.contains(id));
    }
}
