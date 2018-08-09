package com.github.vantonov1.basalt.fulltext.impl;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

import java.security.InvalidParameterException;
import java.util.Collection;

public abstract class AbstractLuceneDAO implements FullTextSearcher {
    private String luceneAnalyzer;
    private Collection<String> tokenized;

    public Collection<String> getTokenized() {
        return tokenized;
    }

    public void setTokenized(Collection<String> tokenized) {
        this.tokenized = tokenized;
    }

    public void setLuceneAnalyzer(String luceneAnalyzer) {
        this.luceneAnalyzer = luceneAnalyzer;
    }

    protected Analyzer getAnalyzer() {
        try {
            return luceneAnalyzer != null ? (Analyzer) Class.forName(luceneAnalyzer).newInstance() : new StandardAnalyzer();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new InvalidParameterException("invalid lucene analyzer " + luceneAnalyzer);
        }
    }
}
