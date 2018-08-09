package com.github.vantonov1.basalt.content.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.UUID;

public class FileContentDAO implements ContentDAO {
    private final String contentRoot;

    public FileContentDAO(String contentRoot) {
        this.contentRoot = contentRoot;
    }

    @Override
    public String create(InputStream content, String suffix) throws IOException {
        final String base = getBasePath();
        final Path dir = Paths.get(contentRoot, base);
        final String name = UUID.randomUUID() + suffix;
        try {
            Files.copy(content, Paths.get(dir.toString(), name));
        } catch (NoSuchFileException ignored) {
            Files.createDirectories(dir);
            Files.copy(content, Paths.get(dir.toString(), name));
        }
        return Paths.get(base, name).toString();
    }

    @Override
    public InputStream get(String path) throws IOException {
        return Files.newInputStream(Paths.get(contentRoot, path));
    }

    @Override
    public long getSize(String path) {
        return Paths.get(contentRoot, path).toFile().length();
    }

    private String getBasePath() {
        final GregorianCalendar calendar = new GregorianCalendar();
        return calendar.get(Calendar.YEAR) + File.separator + (1 + calendar.get(Calendar.MONTH)) + File.separator + calendar.get(Calendar.DAY_OF_MONTH);
    }

}
