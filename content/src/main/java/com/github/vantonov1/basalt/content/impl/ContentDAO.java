package com.github.vantonov1.basalt.content.impl;

import java.io.IOException;
import java.io.InputStream;

public interface ContentDAO {
    String create(InputStream content, String suffix) throws IOException;

    InputStream get(String path) throws IOException;

    long getSize(String path);
}
