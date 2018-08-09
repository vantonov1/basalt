package com.github.vantonov1.basalt.fulltext.impl;

import com.github.vantonov1.basalt.fulltext.FullTextSearchService;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class FullTextSearchServiceImpl implements FullTextSearchService {
    private final FullTextSearcher fullTextSearcher;

    public FullTextSearchServiceImpl(FullTextSearcher fullTextSearcher) {
        this.fullTextSearcher = fullTextSearcher;
    }

    @Override public List<String> search(List<String> mustHaveText, List<String> shouldHaveText, int limit) {
        if(mustHaveText == null && shouldHaveText == null) {
            throw new IllegalArgumentException("search without parameters");
        }
        return fullTextSearcher.search(mustHaveText, shouldHaveText, limit);
    }
}
