package com.github.vantonov1.basalt.fulltext.impl;

import com.github.vantonov1.basalt.content.impl.ContentDAO;
import com.github.vantonov1.basalt.repo.AbstractJdbcDAO;
import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.Pair;
import com.github.vantonov1.basalt.repo.QueryBuilder;
import com.github.vantonov1.basalt.repo.impl.RepositoryDAO;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LuceneIndexer extends AbstractFullTextIndexer {
    public static final String PROP_CONTENT = "__content";

    private final Log logger = LogFactory.getLog(getClass());
    private final LuceneDAO luceneDAO;
    private final RepositoryDAO repositoryDAO;
    private final ContentDAO contentDAO;

    public LuceneIndexer(LuceneDAO luceneDAO, RepositoryDAO repositoryDAO, ContentDAO contentDAO) {
        this.luceneDAO = luceneDAO;
        this.repositoryDAO = repositoryDAO;
        this.contentDAO = contentDAO;
    }

    @Override
    protected void create(String id, List<Pair<String, String>> values) {
        luceneDAO.create(id, values);
    }

    @Override
    protected void create(Map<String, List<Pair<String, String>>> values) {
        luceneDAO.create(values);
    }

    @Override
    protected void updateFields(String id, List<Pair<String, String>> values) {
        luceneDAO.updateFields(id, values);
    }

    @Override
    protected void updateFields(Map<String, List<Pair<String, String>>> values) {
        luceneDAO.updateFields(values);
    }

    @Override
    protected void updateField(String id, String name, String value) {
        luceneDAO.updateFields(id, Collections.singletonList(new Pair<>(name, value)));
    }

    @Override
    protected void removeField(String id) {
        luceneDAO.remove(id);
    }

    @PostConstruct
    private void postConstruct() {
        if (!luceneDAO.check() && luceneDAO.clean()) {
            reindex();
        }
    }

    public void reindex() {
        final List<String> hasFullText = repositoryDAO.queryByParents(new QueryBuilder().isNotNull(tokenized).build(), null, -1);
        if (!hasFullText.isEmpty()) {
            logger.info("reindex fields... " + hasFullText.size());
            for (final List<String> batch : AbstractJdbcDAO.partition(hasFullText)) {
                postponedThreads.submit(() -> {
                    for (Node node : repositoryDAO.getNodes(batch)) {
                        final List<Pair<String, String>> values = getTextValues(node);
                        if (!values.isEmpty()) {
                            create(node.id, values);
                        }
                    }
                    return null;
                });
            }
            awaitTermination();
            luceneDAO.flush();
            logger.info("finished reindex fields");
        }
        final List<String> hasAttachments = repositoryDAO.queryByParents(new QueryBuilder().isNotNull(PROP_CONTENT).build(), null, -1);
        if (!hasAttachments.isEmpty()) {
            logger.info("reindex content..." + hasAttachments.size());
            for (final List<String> batch : AbstractJdbcDAO.partition(hasAttachments)) {
                postponedThreads.submit(() -> {
                    final Map<String, Object> content = repositoryDAO.getProperty(batch, PROP_CONTENT);
                    for (Map.Entry<String, Object> entry : content.entrySet()) {
                        try {
                            indexContent(entry.getKey(), contentDAO.get((String) entry.getValue()));
                        } catch (IOException e) {
                            logger.info("while reindex content", e);
                        }
                    }
                });
            }
            awaitTermination();
            logger.info("finished reindex content");
        }
    }

    public void awaitTermination() {//public for tests
        super.awaitTermination();
        luceneDAO.flush();
    }

}
