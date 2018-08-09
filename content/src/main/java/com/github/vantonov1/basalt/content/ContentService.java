package com.github.vantonov1.basalt.content;

import com.github.vantonov1.basalt.repo.Node;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.io.File;

/**
 * Manages unstructured {@link Content} in repository. Arbitrary content could be attached to any {@link Node Node} in repository.
 * <p>Implementation is pluggable, for now content is stored on the file system or (for testing) in memory. Exact place is configured by <code>content.root</code> system property - set it to the absolute path on file system, or "mem" to use  memory storage. Alternatively, implement {@link com.github.vantonov1.basalt.BasaltContentConfiguration.BasaltContentConfigurationCallback configuration callback}</p>
 * <p>Injects optional full-text search indexer and calls its methods to keep index in sync</p>
 */
public interface ContentService {
    /**
     * Attach content from the file to existing node. Meta information (content type and locale) guessed using Apache Tika
     * @param id node GUID
     * @param file any file on local file system
     */
    void putContent(@NonNull String id, @NonNull File file);

    /**
     * Attach content to existing node
     * @param id node GUID
     * @param content input stream and meta information (content type and encoding). Content size is calculated automatically
     */
    void putContent(@NonNull String id, @NonNull Content content);

    /**
     * Copy content meta information from one node to another. Content itself is not copied, both nodes will have same content attached
     * @param from node GUID
     * @param to node GUID
     * @param index should full-text indexer be called. Set to false, for example, to create node version (it should not be searched)
     */
    void copyContent(@NonNull String from, @NonNull String to, boolean index);

    /**
     * Get content attached to node, or null
     * @param id node GUID
     * @return content description, including input stream. Stream MUST be closed after usage
     */
    @Nullable Content getContent(@NonNull String id);

    /**
     * Utility method to get file mime type by its extension (Apache Tika is used)
     */
    String getMimeType(@Nullable String filename);
}
