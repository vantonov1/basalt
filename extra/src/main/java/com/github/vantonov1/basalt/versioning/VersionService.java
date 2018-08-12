package com.github.vantonov1.basalt.versioning;

import com.github.vantonov1.basalt.content.ContentService;
import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.NodeService;
import com.github.vantonov1.basalt.repo.impl.GUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Component
public class VersionService {
    private static final String PROP_CURRENT_VERSION = "__currentVersion";
    private static final String PROP_VERSION_OF = "__versionOf";

    private final NodeService nodeService;
    private final ContentService contentService;

    @Autowired
    public VersionService(NodeService nodeService, ContentService contentService) {
        this.nodeService = nodeService;
        this.contentService = contentService;
    }

    public String createVersion(Node node) {
        final List<Node> nodes = convertToVersions(Collections.singletonList(node));
        assert nodes.size() == 1;
        final Node version = nodes.get(0);
        final String versionId = nodeService.createNode(node.id, version, null, null);
        contentService.copyContent(node.id, versionId, false);
        nodeService.setProperty(node.id, PROP_CURRENT_VERSION, versionId);
        return versionId;
    }

    public Map<String, Collection<String>> createVersion(List<Node> nodes) {
        final Map<String, Collection<Node>> versionNodesById = new HashMap<>();
        convertToVersions(nodes).forEach(node -> versionNodesById.computeIfAbsent(node.get(PROP_VERSION_OF), k -> new ArrayList<>()).add(node));
        final Map<String, Collection<String>> versionIdsByParent = nodeService.createNodes(versionNodesById, null);
        for (Map.Entry<String, Collection<String>> entry : versionIdsByParent.entrySet()) {
            final String id = entry.getKey();
            for (String versionId : entry.getValue()) {
                contentService.copyContent(id, versionId, false);
                nodeService.setProperty(id, PROP_CURRENT_VERSION, versionId);
            }
        }
        return versionIdsByParent;
    }

    public String getHead(String id) {
        return nodeService.getProperty(id, PROP_VERSION_OF);
    }

    public String getCurrentVersion(String id) {
        return nodeService.getProperty(id, PROP_CURRENT_VERSION);
    }

    public List<String> getHeads(Collection<String> ids) {
        return newArrayList((Iterable) nodeService.getProperty(ids, PROP_VERSION_OF).values());
    }

    public List<String> getCurrentVersions(Collection<String> ids) {
        return newArrayList((Iterable) nodeService.getProperty(ids, PROP_CURRENT_VERSION).values());
    }

    public List<String> getAllVersionIds(String id) {
        return nodeService.getVersions(null, PROP_VERSION_OF, id);
    }

    public List<String> getAllTypeVersions(String type) {
        return nodeService.getVersions(type, null, null);
    }

    public List<Node> convertToVersions(List<Node> nodes) {
        final Map<String, String> guids = new HashMap<>();
        final List<Node> versionNodes = new ArrayList<>(nodes.size());
        for (Node node : nodes) {
            final Node version = new Node(node.type, null);
            version.version = true;
            for (Map.Entry<String, Serializable> entry : node.getProperties().entrySet()) {
                if (GUID.is(entry.getValue())) {
                    guids.put(entry.getKey(), (String) entry.getValue());
                } else {
                    version.put(entry.getKey(), entry.getValue());
                }
            }
            version.put(PROP_VERSION_OF, node.id);
            versionNodes.add(version);
        }
        if (!guids.isEmpty()) {
            final Map<String, Object> currentVersions = nodeService.getProperty(guids.values(), PROP_CURRENT_VERSION);
            for (Node node : versionNodes) {
                for (Map.Entry<String, String> entry : guids.entrySet()) {
                    final Object currentVersion = currentVersions.get(entry.getValue());
                    node.put(entry.getKey(), currentVersion instanceof String ? (Serializable) currentVersion : entry.getValue());
                }
            }
        }
        return versionNodes;
    }

    private <T> List<T> newArrayList(Iterable<T> nodes) {
        return StreamSupport.stream(nodes.spliterator(), false)
                .collect(Collectors.toList());
    }
}
