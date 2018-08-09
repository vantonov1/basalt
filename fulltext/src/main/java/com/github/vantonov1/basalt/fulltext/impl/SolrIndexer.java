package com.github.vantonov1.basalt.fulltext.impl;

import com.github.vantonov1.basalt.repo.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class SolrIndexer extends AbstractFullTextIndexer {
    private SolrDAO solrDAO;

    public SolrIndexer(SolrDAO solrDAO) {
        this.solrDAO = solrDAO;
    }

    @Override
    protected void create(String id, List<Pair<String, String>> values) {
        solrDAO.create(id, values);
    }

    @Override
    protected void create(Map<String, List<Pair<String, String>>> values) {
        solrDAO.create(values);
    }

    @Override
    protected void updateFields(String id, List<Pair<String, String>> values) {
        solrDAO.updateFields(id, values);
    }

    @Override
    protected void updateFields(Map<String, List<Pair<String, String>>> values) {
        solrDAO.updateFields(values);
    }

    @Override
    protected void updateField(String id, String name, String value) {
        solrDAO.updateFields(id, Collections.singletonList(new Pair<>(name, value)));
    }

    @Override
    protected void removeField(String id) {
        solrDAO.remove(id);
    }

    @Override
    public void afterCommit() {
        super.afterCommit();
        solrDAO.commit();
    }

    @Override
    public void awaitTermination() {
        super.awaitTermination();
        solrDAO.commit();
    }
}
