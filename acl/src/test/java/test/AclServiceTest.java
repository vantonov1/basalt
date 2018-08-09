package test;

import com.github.vantonov1.basalt.acl.AclService;
import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.NodeService;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class AclServiceTest extends BaseTest {
    @Autowired
    private AclService aclService;

    @Autowired
    private NodeService nodeService;

    private String f1;
    private String f2;
    private String d1;
    private String d2;
    private String d3;

    @Before
    public void init() throws SQLException {
        final Object tx = beginTx(false);
        final Node folder = new Node("folder", null);
        f1 = nodeService.createNode(null, folder, null, null);
        f2 = nodeService.createNode(f1, folder, null, null);

        final Node doc = new Node("doc", Collections.<String, Serializable>singletonMap("dateValue", new Date()));
        d1 = nodeService.createNode(f1, doc, null, null);
        d2 = nodeService.createNode(f2, doc, null, null);
        d3 = nodeService.createNode(f2, doc, null, null);

        aclService.setAcl(d1, "a1", AclService.MASK_READ);
        aclService.setAcl(f1, "a1", AclService.MASK_READ);
        aclService.setAcl(f2, "a2", AclService.MASK_READ | AclService.MASK_WRITE);
        aclService.setAcl(d3, "a3", AclService.MASK_READ | AclService.MASK_DELETE);
        commit(tx);
    }

    @Test
    public void testAccess() throws SQLException {
        final Object tx = beginTx(true);

        Assert.assertTrue(aclService.isReadableBy(d1, "a1", null));//direct
        Assert.assertTrue(aclService.isReadableBy(d1, "a2", Collections.singleton("a1")));//direct use membership
        Assert.assertFalse(aclService.isReadableBy(d1, "a2", null));
        Assert.assertTrue(aclService.isReadableBy(d2, "a2", null));//level up

        Assert.assertTrue(aclService.isWritableBy(f2, "a2", null));//direct
        Assert.assertTrue(aclService.isWritableBy(d2, "a2", null));//level up
        Assert.assertFalse(aclService.isWritableBy(d2, "a1", null));

        Assert.assertTrue(aclService.isDeletableBy(d3, "a3", null));
        Assert.assertFalse(aclService.isDeletableBy(d3, "a1", null));

        final List<String> a1 = aclService.filterReadable(Arrays.asList(d1, d2, d3), "a1", null);
        Assert.assertTrue(a1 != null && a1.size() == 1 && a1.contains(d1));

        final List<String> a2 = aclService.filterReadable(Arrays.asList(d1, d2, d3), "a2", null);
        Assert.assertTrue(a2 != null && a2.size() == 2 && !a2.contains(d1));

        final List<String> a1_a3 = aclService.filterReadable(Arrays.asList(d1, d2, d3), "a3", Collections.singleton("a1"));
        Assert.assertTrue(a1_a3 != null && a1_a3.size() == 2 && !a1_a3.contains(d2));
        rollback(tx);
    }

    @Test
    public void testConversion() throws SQLException {
        final Object tx = beginTx(false);

        Assert.assertTrue(aclService.convertToReadonly(f2));
        Assert.assertFalse(aclService.isWritableBy(d2, "a2", null));
        Assert.assertTrue(aclService.isReadableBy(d2, "a2", null));

        Assert.assertTrue(aclService.convertToReadWrite(f2));
        Assert.assertTrue(aclService.isWritableBy(d2, "a2", null));
        Assert.assertTrue(aclService.isReadableBy(d2, "a2", null));
        rollback(tx);
    }


    @Test
    public void testACLs() throws SQLException {
        final Object tx = beginTx(false);

        Map<String, Integer> acl = aclService.getAcl(d3);
        Assert.assertTrue(acl != null && acl.size() == 1);
        Assert.assertTrue(acl.get("a3") == (AclService.MASK_READ | AclService.MASK_DELETE));

        acl = aclService.getAcl(d2);
        Assert.assertTrue(acl != null && acl.size() == 1);
        Assert.assertTrue(acl.get("a2") == (AclService.MASK_READ | AclService.MASK_WRITE));

        aclService.addAcl(f2, Collections.singletonMap("a3", AclService.MASK_READ | AclService.MASK_DELETE));
        acl = aclService.getAcl(d2);
        Assert.assertTrue(acl != null && acl.size() == 2);
        Assert.assertTrue(acl.get("a1") == null);
        Assert.assertTrue(acl.get("a2") == (AclService.MASK_READ | AclService.MASK_WRITE));
        Assert.assertTrue(acl.get("a3") == (AclService.MASK_READ | AclService.MASK_DELETE));

        aclService.setAcl(f2, Collections.singletonMap("a1", AclService.MASK_READ));
        acl = aclService.getAcl(d2);
        Assert.assertTrue(acl != null && acl.size() == 1);
        Assert.assertTrue(acl.get("a1") == AclService.MASK_READ);
        Assert.assertTrue(acl.get("a2") == null);

        aclService.setAcl(f1, null);
        acl = aclService.getAcl(f1);
        Assert.assertNull(acl);

        rollback(tx);
    }
}
