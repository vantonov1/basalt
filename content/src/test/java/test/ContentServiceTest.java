package test;

import com.github.vantonov1.basalt.content.Content;
import com.github.vantonov1.basalt.content.ContentService;
import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.NodeService;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.util.Collections;

public class ContentServiceTest extends BaseTest {
    @Autowired
    private NodeService nodeService;

    @Autowired
    private ContentService contentService;

    private final byte[] garbage = new byte[100500];

    @Test
    public void testNodeCRUD() throws SQLException, IOException, URISyntaxException {
        final Object tx = beginTx(false);
        final String id1 = nodeService.createNode(null, new Node("content", Collections.singletonMap("title", (Serializable) "file with content")), null, null);
        final File file1 = new File(new URI(getClass().getResource("/test-content.txt").toString()));
        contentService.putContent(id1, file1);

        for (int i1 = 0; i1 < 100500; i1++) {
            garbage[i1] = (byte) (i1 & 0x7F);
        }
        final Content big = new Content();
        big.stream = new ByteArrayInputStream(garbage);
        big.size = 100500;
        big.type = "application/octet-stream";

        final String id2 = nodeService.createNode(null, new Node("content", null), null, null);
        contentService.putContent(id2, big);

        final Content c1 = contentService.getContent(id1);
        Assert.assertNotNull(c1);
        Assert.assertEquals("text/plain", c1.type);
        Assert.assertEquals("text/plain", contentService.getMimeType(file1.getAbsolutePath()));
//        Assert.assertEquals("UTF-8", content.encoding);
        Assert.assertEquals(12, c1.size);
        Assert.assertEquals(12, c1.stream.available());

        final File file = new File(new URI(getClass().getResource("/test-content.txt").toString()));
        final BufferedReader fileReader = new BufferedReader(new FileReader(file));
        final BufferedReader contentReader = new BufferedReader(new InputStreamReader(c1.stream));

        for (String original = fileReader.readLine(); original != null; original = fileReader.readLine()) {
            final String line = contentReader.readLine();
            Assert.assertNotNull(line);
            Assert.assertEquals(original, line);
        }

        final Content c2 = contentService.getContent(id2);
        Assert.assertNotNull(c2);
        Assert.assertEquals("application/octet-stream", c2.type);
        Assert.assertEquals(100500, c2.size);

        for (int i = 0; i < 100500; i++) {
            Assert.assertTrue(c2.stream.available() > 0);
            final int c = c2.stream.read();
            Assert.assertEquals(garbage[i], c);
        }

        contentService.copyContent(id1, id2, true);
        final Content c3 = contentService.getContent(id2);
        Assert.assertNotNull(c3);
        Assert.assertEquals("text/plain", c3.type);
        Assert.assertEquals(12, c3.size);
        Assert.assertEquals(12, c3.stream.available());
        commit(tx);
    }

}
