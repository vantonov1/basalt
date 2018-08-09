package test;

import org.junit.Test;

import java.sql.SQLException;

public class VersionServiceTest extends BaseTest {
//    @Autowired
//    private VersionService versionService;
//
//    @Autowired
//    private NodeService nodeService;
//
    @Test
    public void testCreate() throws SQLException {
//        final Object tx = beginTx(false);
//        final String id = nodeService.createNode(null, new Node("typeA", Collections.singletonMap("name", "abc")), null, null);
//        final String versionId = versionService.createVersion(nodeService.getProperties(id));
//        Assert.assertEquals(id, versionService.getHead(versionId));
//        Assert.assertEquals(versionId, versionService.getCurrentVersion(id));
//
//        final String id2 = nodeService.createNode(null, new Node("typeA", Collections.singletonMap("ref", id)), null, null);
//        final String versionId2 = versionService.createVersion(nodeService.getProperties(id2));
//
//        final Node versioned = nodeService.getProperties(versionId2);
//        Assert.assertTrue(versioned.version);
//        Assert.assertEquals(versionId, versioned.get("ref"));
//
//        final String secondVersion = versionService.createVersion(nodeService.getProperties(id));
//        Assert.assertEquals(secondVersion, versionService.getCurrentVersion(id));
//        Assert.assertEquals(id, versionService.getHead(secondVersion));
//        rollback(tx);
    }

    @Test
    public void testBulk() throws SQLException {
//        final Object tx = beginTx(false);
//        final String id = nodeService.createNode(null, new Node("typeA", Collections.singletonMap("name", "abc")), null, null);
//        final String id2 = nodeService.createNode(null, new Node("typeA", Collections.singletonMap("name", "def")), null, null);
//        final Map<String, Collection<String>> versionIds = versionService.createVersion(nodeService.getProperties(Arrays.asList(id, id2)));
//        final List<String> allVersionIds = versionIds.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
//        Assert.assertTrue(versionIds.containsKey(id) && versionIds.containsKey(id2));
//        final List<String> heads = versionService.getHeads(allVersionIds);
//        Assert.assertTrue(heads.contains(id) && heads.contains(id2));
//        List<String> currentVersions = versionService.getCurrentVersions(Arrays.asList(id, id2));
//        Assert.assertTrue(currentVersions.contains(allVersionIds.iterator().next()));
//
//        final String secondVersion = versionService.createVersion(nodeService.getProperties(id));
//
//        final List<String> versions = versionService.getAllVersionIds(id);
//        Assert.assertEquals(2, versionIds.size());
//        Assert.assertTrue(versions.contains(secondVersion));
//        currentVersions = versionService.getCurrentVersions(Arrays.asList(id, id2));
//        Assert.assertTrue(currentVersions.contains(secondVersion));
//
//        final List<String> allTypeVersions = versionService.getAllTypeVersions("typeA");
//        Assert.assertEquals(3, allTypeVersions.size());
//        Assert.assertTrue(allTypeVersions.contains(secondVersion));
//        rollback(tx);
    }
}
