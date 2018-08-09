package com.github.vantonov1.basalt;

import com.github.vantonov1.basalt.content.impl.ContentDAO;
import com.github.vantonov1.basalt.fulltext.impl.AbstractFullTextIndexer;
import com.github.vantonov1.basalt.fulltext.impl.FullTextSearcher;
import com.github.vantonov1.basalt.fulltext.impl.LuceneDAO;
import com.github.vantonov1.basalt.fulltext.impl.LuceneIndexer;
import com.github.vantonov1.basalt.fulltext.impl.MemoryLuceneDAO;
import com.github.vantonov1.basalt.fulltext.impl.SolrDAO;
import com.github.vantonov1.basalt.fulltext.impl.SolrIndexer;
import com.github.vantonov1.basalt.repo.FullTextIndexer;
import com.github.vantonov1.basalt.repo.impl.RepositoryDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

@Configuration
@ComponentScan
public class BasaltFullTextConfiguration {
    public interface BasaltFullTextConfigurationCallback {
        String getLuceneRoot();

        String getSolrCoreUrl();

        String getLuceneAnalyzer();

        Collection<String> getFullTextFields();
    }

    private final Environment environment;
    private final RepositoryDAO repositoryDAO;
    private final ContentDAO contentDAO;

    private BasaltFullTextConfigurationCallback configurationCallback;
    private FullTextSearcher searcher;
    private AbstractFullTextIndexer indexer;

    public BasaltFullTextConfiguration(Environment environment, RepositoryDAO repositoryDAO, ContentDAO contentDAO) {
        this.environment = environment;
        this.repositoryDAO = repositoryDAO;
        this.contentDAO = contentDAO;
    }

    @Autowired(required = false)
    public void setConfigurationCallback(BasaltFullTextConfigurationCallback configurationCallback) {
        this.configurationCallback = configurationCallback;
    }

    @PostConstruct
    private void init() {
        final String luceneAnalyzer = configurationCallback != null ? configurationCallback.getLuceneAnalyzer() : environment.getProperty("lucene.analyzer");
        final String luceneRoot = configurationCallback != null ? configurationCallback.getLuceneRoot() : environment.getProperty("lucene.root");
        final String solrUrl = configurationCallback != null ? configurationCallback.getSolrCoreUrl() : environment.getProperty("solr.core");
        final Collection<String> fullTextFields = getFullTextFields();
        if (solrUrl != null) {
            final SolrDAO solrDAO = new SolrDAO(solrUrl);
            solrDAO.setLuceneAnalyzer(luceneAnalyzer);
            solrDAO.setTokenized(fullTextFields);
            searcher = solrDAO;
            indexer = new SolrIndexer(solrDAO);
        } else if (luceneRoot != null) {
            final LuceneDAO luceneDAO = "mem".equalsIgnoreCase(luceneRoot) ? new MemoryLuceneDAO() : new LuceneDAO();
            luceneDAO.setLuceneRoot(luceneRoot);
            luceneDAO.setLuceneAnalyzer(luceneAnalyzer);
            luceneDAO.setTokenized(fullTextFields);
            searcher = luceneDAO;
            indexer = new LuceneIndexer(luceneDAO, repositoryDAO, contentDAO);
        } else {
            throw new IllegalArgumentException("solr.core or lucene.root have to be provided");
        }
        indexer.setTokenized(fullTextFields);
    }

    @Bean
    public FullTextSearcher getFulltextSearcher() {
        return searcher;
    }

    @Bean
    public FullTextIndexer getFullTextIndexer() {
        return indexer;
    }

    private Collection<String> getFullTextFields() {
        final String f = environment.getProperty("fulltext.fields");
        return configurationCallback != null
                ? configurationCallback.getFullTextFields()
                : f != null
                    ? new HashSet<>(Arrays.asList(f.split(",")))
                    : Collections.emptyList();
    }
}
