package com.github.vantonov1.basalt.content;

import com.github.vantonov1.basalt.repo.Node;

import java.io.InputStream;

/**
 * Description of content attached to {@link Node Node}
 */
public class Content {
    public String type;
    public String encoding;
    public InputStream stream;
    public long size;
}

