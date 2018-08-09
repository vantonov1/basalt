package com.github.vantonov1.basalt.content.impl;

import org.apache.commons.io.IOUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class MemoryContentDAO  implements ContentDAO {
    private final Map<String, byte[]> contents = new ConcurrentHashMap<>();

    public String create(InputStream content, String suffix) throws IOException {
            final String id = UUID.randomUUID().toString();
            contents.put(id, IOUtils.toByteArray(content));
            return id;
    }

    public InputStream get(String path) {
            final byte[] content = contents.get(path);
            return content != null ? new ByteArrayInputStream(content) : null;
    }

    public long getSize(String path) {
            final byte[] bytes = contents.get(path);
            return bytes != null ? bytes.length : 0;
    }
}
