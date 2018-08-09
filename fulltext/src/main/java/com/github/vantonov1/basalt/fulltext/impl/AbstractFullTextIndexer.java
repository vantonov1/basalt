package com.github.vantonov1.basalt.fulltext.impl;

import com.github.vantonov1.basalt.fulltext.FullTextSearchService;
import com.github.vantonov1.basalt.repo.FullTextIndexer;
import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.ToTextContentHandler;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public abstract class AbstractFullTextIndexer extends TransactionSynchronizationAdapter implements FullTextIndexer {
    private static final String SEPARATORS = " \\-\r\n.,;:^!?@\"\'#$%&*()[]{}\\/";

    private final Log logger = LogFactory.getLog(getClass());

    protected ExecutorService postponedThreads = new ThreadPoolExecutor(0, 16, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
    ;
    protected Collection<String> tokenized = Collections.emptyList();

    protected abstract void create(String id, List<Pair<String, String>> values);

    protected abstract void create(Map<String, List<Pair<String, String>>> values);

    protected abstract void updateFields(String node, List<Pair<String, String>> values);

    protected abstract void updateFields(Map<String, List<Pair<String, String>>> values);

    protected abstract void updateField(String id, String name, String value);

    protected abstract void removeField(String id);

    @Override
    public void create(final String id, final Node node) {
        final List<Pair<String, String>> values = getTextValues(node);
        if (!values.isEmpty()) {
            postponeIndexFields(() -> create(id, values));
        }
    }

    @Override
    public void create(Collection<String> ids, Collection<Node> nodes) {
        final Map<String, List<Pair<String, String>>> values = new HashMap<>();
        final Iterator<Node> it = nodes.iterator();
        for (String id : ids) {
            final Node node = it.next();
            for (String prop : tokenized) {
                final String value = node.get(prop);
                if (value != null && !value.isEmpty()) {
                    values.computeIfAbsent(id, v -> new ArrayList<>()).add(new Pair<>(prop, value));
                }
            }
        }
        if (!values.isEmpty()) {
            postponeIndexFields(() -> create(values));
        }
    }

    @Override
    public void update(Node node, boolean deleteOld) {
        final List<Pair<String, String>> values = new ArrayList<>();
        for (String prop : tokenized) {
            final String value = node.get(prop);
            if (value != null && !value.isEmpty() || deleteOld) {
                values.add(new Pair<>(prop, value));
            }
        }
        if (!values.isEmpty()) {
            postponeIndexFields(() -> updateFields(node.id, values));
        }
    }

    @Override
    public void update(Collection<Node> nodes, boolean deleteOld) {
        final Map<String, List<Pair<String, String>>> values = new HashMap<>();
        for (Node node : nodes) {
            for (String prop : tokenized) {
                final String value = node.get(prop);
                if (value != null && !value.isEmpty() || deleteOld) {
                    values.computeIfAbsent(node.id, id -> new ArrayList<>()).add(new Pair<>(prop, value));
                }
            }
        }
        if (!values.isEmpty()) {
            postponeIndexFields(() -> updateFields(values));
        }
    }

    @Override
    public void update(final String id, String name, String value) {
        if (tokenized.contains(name)) {
            postponeIndexFields(() -> updateField(id, name, value));
        }
    }

    @Override
    public void update(final String id, final InputStream content) {
        postpone(() -> indexContent(id, content));
    }

    @Override
    public void remove(final String id) {
        postpone(() -> {
            try {
                removeField(id);
            } catch (RuntimeException e) {
                logger.error("while deleting", e);
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public void afterCommit() {
        final List<Runnable> postponed = (List<Runnable>) TransactionSynchronizationManager.getResource("LuceneTask");
        if (postponed != null) {
            for (final Runnable runnable : postponed) {
                try {
                    postponedThreads.submit(runnable);
                } catch (Exception ignored) {
                }
            }
        }
    }

    @Override
    public void afterCompletion(int i) {
        if (TransactionSynchronizationManager.hasResource("LuceneTask")) {
            TransactionSynchronizationManager.unbindResource("LuceneTask");
        }
    }

    public void awaitTermination() {
        try {
            postponedThreads.shutdown();
            postponedThreads.awaitTermination(Integer.MAX_VALUE, TimeUnit.SECONDS);
            postponedThreads = Executors.newFixedThreadPool(16);
        } catch (InterruptedException ignored) {
        }
    }

    protected List<Pair<String, String>> getTextValues(Node node) {
        final List<Pair<String, String>> values = new ArrayList<>();
        for (String prop : tokenized) {
            final String value = node.get(prop);
            if (value != null && !value.isEmpty()) {
                values.add(new Pair<>(prop, value));
            }
        }
        return values;
    }

    protected void indexContent(String id, InputStream stream) {
        if (stream != null) {
            try (InputStream s = stream) {
                updateField(id, FullTextSearchService.SEARCH_CONTENT, String.join(" ", extractText(s)));
            } catch (Exception e) {
                logger.error( "while indexing content", e);
            }
        }
    }

    private Collection<String> extractText(InputStream s) throws IOException, SAXException, TikaException {
        final Set<String> result = new HashSet<>();
        try (final ParseOutputStream wordsStream = new ParseOutputStream(result)) {
            final ParseContext context = new ParseContext();
            final TikaConfig tikaConfig = new TikaConfig();
            final Parser parser = new AutoDetectParser(tikaConfig);
            context.set(Parser.class, parser);
            parser.parse(s, new ToTextContentHandler(wordsStream, "UTF-8"), new Metadata(), context);
        } catch (RuntimeException e) {
            logger.error( "while parsing content", e);
        }
        return result;
    }

    private void postponeIndexFields(Runnable task) {
        postpone(() -> {
            try {
                task.run();
            } catch (RuntimeException e) {
                logger.error( "while indexing fields", e);
            }
        });
    }

    @SuppressWarnings("unchecked")
    private void postpone(Runnable task) {
        if (TransactionSynchronizationManager.isActualTransactionActive()) {
            List<Runnable> postponed = (List<Runnable>) TransactionSynchronizationManager.getResource("LuceneTask");
            if (postponed == null) {
                postponed = new ArrayList<>();
                TransactionSynchronizationManager.bindResource("LuceneTask", postponed);
                TransactionSynchronizationManager.registerSynchronization(this);
            }
            postponed.add(task);
        } else {
            task.run();
        }
    }

    public void setTokenized(Collection<String> tokenized) {
        this.tokenized = tokenized;
    }

    private static class ParseOutputStream extends OutputStream {
        private final Set<String> words;
        private final byte[] bytes = new byte[1024];
        private int count = 0;

        private ParseOutputStream(Set<String> words) {
            this.words = words;
        }

        @Override
        public void write(int b) throws IOException {
            if (SEPARATORS.indexOf(b) != -1 || count == 1024) {
                addWord();
            } else {
                bytes[count++] = (byte) (b & 0xFF);
            }
        }

        @Override
        public void close() throws IOException {
            addWord();
        }

        private void addWord() throws UnsupportedEncodingException {
            if (count > 0) {
                words.add(new String(bytes, 0, count, "UTF-8"));
                count = 0;
            }
        }
    }
}
