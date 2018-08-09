package com.github.vantonov1.basalt.fulltext.impl;

import com.github.vantonov1.basalt.repo.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.ConcurrentUpdateSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class SolrDAO extends AbstractLuceneDAO {
    private final Log logger = LogFactory.getLog(getClass());
    private final SolrClient updateClient;
    private final HttpSolrClient queryClient;

    public SolrDAO(String coreUrl) {
        updateClient = new ConcurrentUpdateSolrClient.Builder(coreUrl).withQueueSize(10).build();
        queryClient = new HttpSolrClient.Builder(coreUrl).build();
    }

    @Override
    public List<String> search(List<String> mustHave, List<String> shouldHave, int limit) {
        try {
            final String query = new LuceneQueryBuilder(getAnalyzer(), getTokenized()).build(mustHave, shouldHave).toString();
            final SolrQuery solrQuery = new SolrQuery(query);
            if (limit > 0) {
                solrQuery.setRows(limit);
            }
            final QueryResponse entries = queryClient.query(solrQuery);
            return entries.getResults().stream()
                    .map(document -> document.getFieldValue("id"))
                    .filter(Objects::nonNull)
                    .map(Object::toString)
                    .collect(Collectors.toList());
        } catch (SolrServerException | IOException e) {
            throw new RuntimeException("while searching solr index", e);
        }
    }

    public void create(String id, List<Pair<String, String>> values) {
        try {
            updateClient.add(createSolrDoc(id, values));
        } catch (SolrServerException | IOException e) {
            throw new RuntimeException("while creating solr index", e);
        }
    }

    public void create(Map<String, List<Pair<String, String>>> values) {
        try {
            updateClient.add(values.entrySet().stream().map(e -> createSolrDoc(e.getKey(), e.getValue())).collect(Collectors.toList()));
        } catch (IOException | SolrServerException e) {
            throw new RuntimeException("while creating solr index", e);
        }
    }

    public void updateFields(String id, List<Pair<String, String>> values) {
        if (!values.isEmpty()) {
            try {
                final SolrInputDocument doc = new SolrInputDocument();
                doc.addField("id", id);
                for (Pair<String, String> entry : values) {
                    doc.addField(entry.getFirst(), Collections.singletonMap("set", entry.getSecond()));
                }
                updateClient.add(doc);
            } catch (IOException | SolrServerException e) {
                throw new RuntimeException("while updating solr index", e);
            }
        }
    }

    public void updateFields(Map<String, List<Pair<String, String>>> values) {
        if (!values.isEmpty()) {
            for (Map.Entry<String, List<Pair<String, String>>> entry : values.entrySet()) {
                updateFields(entry.getKey(), entry.getValue());
            }
        }
    }

    public void remove(String id) {
        try {
            updateClient.deleteById(id);
        } catch (IOException | SolrServerException e) {
            throw new RuntimeException("while deleting solr index", e);
        }
    }

    public void commit() {
        try {
            updateClient.commit(false, false);
        } catch (SolrServerException | IOException e) {
            logger.error("while commiting to Solr", e);
        }
    }

    private static SolrInputDocument createSolrDoc(String id, List<Pair<String, String>> props) {
        final SolrInputDocument doc = new SolrInputDocument();
        doc.addField("id", id);
        for (Pair<String, String> nameAndValue : props) {
            if (nameAndValue.getSecond() != null) {
                doc.addField(nameAndValue.getFirst(), nameAndValue.getSecond());
            }
        }
        return doc;
    }
}
