package com.github.vantonov1.basalt.fulltext.impl;

import com.github.vantonov1.basalt.fulltext.FullTextSearchService;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.PrefixQuery;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.BiConsumer;

public class LuceneQueryBuilder {
    private final Analyzer luceneAnalyzer;
    private final Collection<String> tokenized;

    public LuceneQueryBuilder(Analyzer luceneAnalyzer, Collection<String> tokenized) {
        this.luceneAnalyzer = luceneAnalyzer;
        this.tokenized = tokenized;
    }

    @SuppressWarnings("unused")
    protected BooleanQuery build(List<String> mustHave, List<String> shouldHave) {
        try (Analyzer analyzer = luceneAnalyzer) {
            final BooleanQuery.Builder queryBuilder = new BooleanQuery.Builder();
            addMustHave(queryBuilder, mustHave);
            addShouldHave(queryBuilder, shouldHave);
            return queryBuilder.build();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void addShouldHave(BooleanQuery.Builder queryBuilder, List<String> phrase) throws IOException {
        if (phrase != null && !phrase.isEmpty()) {
            final BooleanQuery.Builder shouldPhraseBuilder = new BooleanQuery.Builder();
            parse(phrase, (word, field) -> {
                if (field.isEmpty()) {
                    addWord(shouldPhraseBuilder, word, FullTextSearchService.SEARCH_CONTENT);
                    for (String t : tokenized) {
                        addWord(shouldPhraseBuilder, word, t);
                    }
                } else {
                    addWord(shouldPhraseBuilder, word, field);
                }
            });
            queryBuilder.add(shouldPhraseBuilder.build(), BooleanClause.Occur.MUST);
        }
    }

    private void addMustHave(BooleanQuery.Builder queryBuilder, List<String> phrase) throws IOException {
        if (phrase != null && !phrase.isEmpty()) {
            parse(phrase, (word, field) -> {
                if (field.isEmpty()) {
                    final BooleanQuery.Builder wordBuilder = new BooleanQuery.Builder();
                    addWord(wordBuilder, word, FullTextSearchService.SEARCH_CONTENT);
                    for (String t : tokenized) {
                        addWord(wordBuilder, word, t);
                    }
                    queryBuilder.add(wordBuilder.build(), BooleanClause.Occur.MUST);
                } else {
                    queryBuilder.add(new PrefixQuery(new Term(field, word)), BooleanClause.Occur.MUST);
                }
            });
        }
    }

    private static void addWord(BooleanQuery.Builder builder, String word, String field) {
        builder.add(new BooleanClause(new PrefixQuery(new Term(field, word)), BooleanClause.Occur.SHOULD));
    }

    private void parse(List<String> phrase, BiConsumer<String, String> consumer) throws IOException {
        for (String w : phrase) {
            final String[] fieldAndText = w.split(":");
            if (fieldAndText.length > 1) {
                parseWord(fieldAndText[1], fieldAndText[0], consumer);
            } else {
                parseWord(fieldAndText[0], "", consumer);
            }
        }
    }

    private void parseWord(String word, String field, BiConsumer<String, String> consumer) throws IOException {
        try (TokenStream tokenStream = luceneAnalyzer.tokenStream("", word)) {
            final CharTermAttribute cattr = tokenStream.addAttribute(CharTermAttribute.class);
            tokenStream.reset();
            while (tokenStream.incrementToken()) {
                consumer.accept(cattr.toString(), field);
            }
            tokenStream.end();
        }
    }
}
