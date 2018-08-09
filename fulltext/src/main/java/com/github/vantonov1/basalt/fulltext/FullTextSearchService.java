package com.github.vantonov1.basalt.fulltext;

import com.github.vantonov1.basalt.BasaltFullTextConfiguration;
import org.springframework.lang.NonNull;
import org.springframework.lang.Nullable;

import java.util.List;

/**
 * Full-text search using configured indexing and search engine. Indexing is pluggable, for now Apache Lucene and Apache Solr are supported.
 * <p>Implementation is selected by setting system properties:</p>
 * <ul>
 * <li><code>solr.core</code> - URL of core to use Apache Solr. It is better to start Solr in schemaless mode, like that: <code>solr start -e schemaless</code></li>
 * <li><code>lucene.root</code> - path to folder for Apache Lucene indexes (will be created automatically)</li>
 * </ul>
 * <p><code>lucene.analyzer</code> system property could be set to full class name of analyzer to be used by Lucene indexing and Lucene and Solr query text parsing.
 * By default {@link org.apache.lucene.analysis.standard.StandardAnalyzer StandardAnalyzer} is used</p>
 * <p>Content, attached to nodes, is always indexed by extracting text (using Apache Tika).
 * Additional list of fields to index, separated by comma, could be set via system property <code>fulltext.fields</code></p>
 * <p>Implement {@link BasaltFullTextConfiguration configuration callback} if you prefer pure Java configuration</p>
 * <p>All updates to full-text index are done only after transaction is committed, and it could take some time to update indexes</p>
 */
public interface FullTextSearchService {
    String SEARCH_CONTENT = "_text_";

    /**
     * Search full-text index. Parameters support Lucene style "must have/should have".
     * Both must have and should have clauses could be of form <code>field:phrase</code> to search in exact field (use <code>FullTextSearchService.SEARCH_CONTENT</code>
     * to search only in content) or just a phrase to search in all indexed fields and content
     * Resulting query would be in form <code>+clause1 AND ... AND (clauseN OR ...)</code>
     * @param mustHaveText list of clauses node must have all to be found
     * @param shouldHaveText list of clauses node should have at least one to be found
     * @param limit max number of results
     * @return list of node GUIDs
     */
    @NonNull List<String> search(@Nullable List<String> mustHaveText, @Nullable List<String> shouldHaveText, int limit);
}
