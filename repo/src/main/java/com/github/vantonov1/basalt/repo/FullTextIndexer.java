package com.github.vantonov1.basalt.repo;

import java.io.InputStream;
import java.util.Collection;

/**
 * Interface for plugged-in fulltext search engines. Repository injects optional bean, implementing that interface, and calls its methods to keep search index in sync
 */
public interface FullTextIndexer {
    void create(String id, Node node);

    void create(Collection<String> ids, Collection<Node> nodes);

    void update(Node node, boolean deleteOld);

    void update(Collection<Node> nodes, boolean deleteOld);

    void update(String id, String name, String value);

    void update(String id, InputStream content);

    void remove(String id);
}
