package com.github.vantonov1.basalt.fulltext.impl;

import com.github.vantonov1.basalt.repo.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexNotFoundException;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Timer;
import java.util.TimerTask;

public class LuceneDAO extends AbstractLuceneDAO {
    private final Timer timer = new Timer("update lucene indexes");
    private final Log logger = LogFactory.getLog(getClass());

    protected Directory directory;

    private String luceneRoot;
    private IndexWriter writer;
    private SearcherManager searcherManager;

    @PostConstruct
    private void postConstruct() {
        timer.schedule(new TimerTask() {
            @Override
            public void run() {
                flush();
            }

        }, 10000, 10000);
    }

    @PreDestroy
    private void preDestroy() {
        timer.cancel();
        try {
            searcherManager.close();
        } catch (IOException e) {
            logger.error("while closing Lucene search manager", e);
        }
        try {
            writer.close();
        } catch (IOException e) {
            logger.error("while closing Lucene writer", e);
        }
        try {
            directory.close();
        } catch (IOException e) {
            logger.error("while closing Lucene directory", e);
        }
    }

    public void setLuceneRoot(String luceneRoot) {
        this.luceneRoot = luceneRoot;
    }

    @Override
    public List<String> search(List<String> mustHave, List<String> shouldHave, int limit) {
        try {
            final SearcherManager manager = getIndexSearcher();
            final IndexSearcher searcher = manager.acquire();
            try {
                final TopDocs docs = searcher.search(new LuceneQueryBuilder(getAnalyzer(), getTokenized()).build(mustHave, shouldHave), limit > 0 ? limit : 100000);
                if (docs != null && docs.totalHits > 0) {
                    final List<String> result = new ArrayList<>((int) docs.totalHits);
                    for (ScoreDoc d : docs.scoreDocs) {
                        final String id = searcher.doc(d.doc).get("id");
                        assert id != null;
                        result.add(id);
                    }
                    return result;
                }
            } catch (IndexNotFoundException e) {
                logger.error("search on empty index", e);
            } finally {
                manager.release(searcher);
            }
            return Collections.emptyList();
        } catch (IOException e) {
            throw new RuntimeException("while searching lucene index", e);
        }
    }

    public boolean check() {
        try {
            final Path path = getLuceneDir();
            if (path.toFile().exists()) {
                try (FSDirectory result = FSDirectory.open(path); CheckIndex checker = new CheckIndex(result)) {
                    checker.setChecksumsOnly(true);
                    final CheckIndex.Status status = checker.checkIndex();
                    return status.clean;
                }
            } else {
                return false;
            }
        } catch (IOException e) {
            return false;
        }
    }

    public boolean clean() {
        final Path path = getLuceneDir();
        if (path.toFile().exists()) {
            for (Path f : path) {
                if (!f.toFile().delete()) {
                    return false;
                }
            }
        }
        return true;
    }

    public void create(String id, List<Pair<String, String>> values) {
        if (!values.isEmpty()) {
            final Document doc = createLuceneDoc(id, values);
            try {
                getIndexWriter().addDocument(doc);
            } catch (IOException e) {
                throw new RuntimeException("while creating lucene index", e);
            }
        }
    }

    public void create(Map<String, List<Pair<String, String>>> values) {
        final Collection<Document> documents = new ArrayList<>();
        for (Map.Entry<String, List<Pair<String, String>>> entry : values.entrySet()) {
            documents.add(createLuceneDoc(entry.getKey(), entry.getValue()));
        }
        try {
            getIndexWriter().addDocuments(documents);
        } catch (IOException e) {
            throw new RuntimeException("while creating lucene index", e);
        }
    }

    public void updateFields(String id, List<Pair<String, String>> values) {
        if (!values.isEmpty()) {
            try {
                final Term docTerm = new Term("id", id);
                Document doc = getLuceneDoc(docTerm);
                if (doc == null) {
                    flush();
                    doc = getLuceneDoc(docTerm);
                }
                if (doc != null) {
                    assert Objects.equals(doc.get("id"), id);
                    doc.removeField(id);
                    doc.add(new StringField("id", id, Field.Store.YES));
                    for (Pair<String, String> entry : values) {
                        final String name = entry.getFirst();
                        doc.removeField(name);
                        if (entry.getSecond() != null) {
                            doc.add(new TextField(name, entry.getSecond(), Field.Store.YES));
                        }
                    }
                    getIndexWriter().updateDocument(docTerm, doc);
                } else {
                    create(id, values);
                }
            } catch (IOException e) {
                throw new RuntimeException("while updating lucene index", e);
            }
        }
    }

    public void updateFields(Map<String, List<Pair<String, String>>> values) {
        if (!values.isEmpty()) {
            try {
                getIndexWriter().deleteDocuments(values.keySet().stream().map(id -> new Term("id", id)).toArray(Term[]::new));
                create(values);
            } catch (IOException e) {
                throw new RuntimeException("while updating lucene index", e);
            }
        }
    }

    public void remove(String id) {
        try {
            getIndexWriter().deleteDocuments(new Term("id", id));
        } catch (IOException e) {
            throw new RuntimeException("while deleting lucene index", e);
        }
    }

    public void flush() {
        try {
            if (writer != null && writer.hasUncommittedChanges()) {
                writer.commit();
            }
        } catch (IOException e) {
            logger.error("while committing", e);
        }
        try {
            if (searcherManager != null) {
                searcherManager.maybeRefresh();
            }
        } catch (IOException e) {
            logger.error("while updating searchers", e);
        }
    }

    private static Document createLuceneDoc(String id, List<Pair<String, String>> props) {
        final Document doc = new Document();
        doc.add(new StringField("id", id, Field.Store.YES));
        for (Pair<String, String> nameAndValue : props) {
            if (nameAndValue.getSecond() != null) {
                doc.add(new TextField(nameAndValue.getFirst(), nameAndValue.getSecond(), Field.Store.YES));
            }
        }
        return doc;
    }

    private Document getLuceneDoc(Term docTerm) throws IOException {
        final SearcherManager manager = getIndexSearcher();
        final IndexSearcher searcher = manager.acquire();
        try {
            final TopDocs topdocs = searcher.search(new TermQuery(docTerm), 1);
            return topdocs != null && topdocs.scoreDocs.length > 0 ? searcher.doc(topdocs.scoreDocs[0].doc) : null;
        } finally {
            manager.release(searcher);
        }
    }

    private synchronized SearcherManager getIndexSearcher() throws IOException {
        if (searcherManager == null) {
            searcherManager = new SearcherManager(getIndexWriter(), false, false, null);
        }
        return searcherManager;
    }

    private synchronized IndexWriter getIndexWriter() throws IOException {
        if (writer == null || !writer.isOpen()) {
            if (writer != null) {
                try {
                    writer.close();
                } catch (Exception ignored) {
                }
            }
            final IndexWriterConfig iwc = new IndexWriterConfig(getAnalyzer());
            iwc.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            writer = new IndexWriter(getDirectory(), iwc);
        }
        return writer;
    }

    protected Directory getDirectory() {
        if (directory == null) {
            try {
                final Path path = getLuceneDir();
                Files.createDirectories(path);
                directory = FSDirectory.open(path);
            } catch (IOException e) {
                throw new RuntimeException("unable to init Lucene directory", e);
            }
        }
        return directory;
    }


    private Path getLuceneDir() {
        if (luceneRoot == null) {
            throw new InvalidParameterException("lucene root not set");
        }
        return Paths.get(luceneRoot);
    }

}
