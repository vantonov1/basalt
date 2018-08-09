package com.github.vantonov1.basalt.fulltext.impl;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

public class MemoryLuceneDAO extends LuceneDAO {
    @Override
    public boolean check() {
        return true;
    }

    @Override
    public boolean clean() {
        try {
            for (String name : directory.listAll()) {
                directory.deleteFile(name);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    protected Directory getDirectory() {
        if (directory == null) {
            directory = new RAMDirectory();
        }
        return directory;
    }
}