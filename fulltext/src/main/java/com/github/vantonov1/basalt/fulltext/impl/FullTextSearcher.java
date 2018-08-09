package com.github.vantonov1.basalt.fulltext.impl;

import java.util.List;

public interface FullTextSearcher {
    List<String> search(List<String> mustHave, List<String> shouldHave, int limit);
}
