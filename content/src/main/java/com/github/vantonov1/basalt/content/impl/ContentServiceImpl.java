package com.github.vantonov1.basalt.content.impl;

import com.github.vantonov1.basalt.content.Content;
import com.github.vantonov1.basalt.content.ContentService;
import com.github.vantonov1.basalt.repo.FullTextIndexer;
import com.github.vantonov1.basalt.repo.Node;
import com.github.vantonov1.basalt.repo.NodeService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tika.Tika;
import org.apache.tika.parser.txt.CharsetDetector;
import org.apache.tika.parser.txt.CharsetMatch;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Service
class ContentServiceImpl implements ContentService {
    public static final String PROP_CONTENT = "__content";
    private static final String PROP_CONTENT_TYPE = "__content_type";
    private static final String PROP_CONTENT_SIZE = "__content_size";
    private static final String PROP_CONTENT_ENCODING = "__content_encoding";

    private final ExecutorService extractText = new ThreadPoolExecutor(0, 16, 60L, TimeUnit.SECONDS, new SynchronousQueue<>());
    private final Log logger = LogFactory.getLog(getClass());
    private final Tika tika = new Tika();
    private final NodeService nodeService;
    private final ContentDAO contentDAO;

    private FullTextIndexer fullTextIndexer;

    public ContentServiceImpl(NodeService nodeService, ContentDAO contentDAO) {
        this.nodeService = nodeService;
        this.contentDAO = contentDAO;
    }

    @Autowired(required = false)
    public void setFullTextIndexer(FullTextIndexer fullTextIndexer) {
        this.fullTextIndexer = fullTextIndexer;
    }

    @Override
    public void putContent(final String id, final File file) {
        if (id == null || file == null) {
            throw new IllegalArgumentException("putContent(" + id + ", " + file + ')');
        }
        try (final InputStream inputStream = Files.newInputStream(file.toPath())) {
            final String contentId = contentDAO.create(inputStream, file.getName());
            if (logger.isTraceEnabled()) {
                logger.trace("store content for node " + id + " from " + file.getName() + " to " + contentId);
            }
            setContentProperties(id, contentId, file.length(), tika.detect(file), getEncoding(file));
            if (fullTextIndexer != null && file.length() > 0) {
                fullTextIndexer.update(id, contentDAO.get(contentId));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void putContent(final String id, Content content) {
        if (id == null || content == null || content.stream == null) {
            throw new IllegalArgumentException("putContent(" + id + ", " + content + ')');
        }
        try (final InputStream stream = content.stream) {
            final String contentId = contentDAO.create(stream, "");
            long size = contentDAO.getSize(contentId);
            if (logger.isTraceEnabled()) {
                logger.trace("update content for node " + id + " to " + contentId);
            }
            setContentProperties(id, contentId, size, content.type, content.encoding);
            if (fullTextIndexer != null) {
                fullTextIndexer.update(id, contentDAO.get(contentId));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void copyContent(String from, String to, boolean index) {
        if (from == null || to == null) {
            throw new IllegalArgumentException("copyContent(" + from + ", " + to + ')');
        }
        final Node fromV = nodeService.getProperties(from);
        final String contentId = fromV.get(PROP_CONTENT);
        if (contentId != null) {
            final long size = fromV.get(PROP_CONTENT_SIZE);
            final String type = fromV.get(PROP_CONTENT_TYPE);
            final String encoding = fromV.get(PROP_CONTENT_ENCODING);
            setContentProperties(to, contentId, size, type, encoding);
            if (fullTextIndexer != null && size > 0 && index) {
                try {
                    fullTextIndexer.update(to, contentDAO.get(contentId));
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public Content getContent(String id) {
        if (id == null) {
            throw new IllegalArgumentException("getContent(null)");
        }
        final Node node = nodeService.getProperties(id);
        final String contentId = node.get(PROP_CONTENT);
        if (contentId != null) {
            final Content content = new Content();
            final Long size = node.get(PROP_CONTENT_SIZE);
            content.size = size != null ? size : 0;
            content.type = node.get(PROP_CONTENT_TYPE);
            content.encoding = node.get(PROP_CONTENT_ENCODING);
            try {
                content.stream = contentDAO.get(contentId);
            } catch (IOException e) {
                return null;
            }
            return content;
        }
        return null;
    }

    @Override
    public String getMimeType(String filename) {
        if (filename == null) {
            throw new IllegalArgumentException("getMimeType(null)");
        }
        return tika.detect(filename);
    }

    private String getEncoding(final File file) {
        try (InputStream inputStream = new BufferedInputStream(new FileInputStream(file), 12 * 1024)) {
            final CharsetMatch detected = new CharsetDetector().setText(inputStream).detect();
            return detected != null ? detected.getName() : null;
        } catch (IOException e) {
            logger.error("while getting encoding", e);
            return null;
        }
    }

    private void setContentProperties(final String id, final String contentId, final long size, final String mimeType, final String encoding) {
        final Node params = new Node(id);
        params.put(PROP_CONTENT, contentId);
        params.put(PROP_CONTENT_SIZE, size);
        if (mimeType != null) {
            params.put(PROP_CONTENT_TYPE, mimeType);
        }
        if (encoding != null) {
            params.put(PROP_CONTENT_ENCODING, encoding);
        }
        nodeService.updateProperties(params, false);
    }

}
