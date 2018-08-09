package com.github.vantonov1.basalt;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.concurrent.ConcurrentMapCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Configuration
@ComponentScan
@EnableCaching
public class BasaltRepoConfiguration {
    @Autowired
    private DataSource dataSource;

    @PostConstruct
    private void postConstruct() {
        initSchema();
    }

    @Bean
    @ConditionalOnMissingBean(CacheManager.class)
    public CacheManager cacheManager() {
        return new ConcurrentMapCacheManager();
    }

    private void initSchema() {
        try (Connection c = dataSource.getConnection()) {
            final ClassPathResource schema = new ClassPathResource("schema.sql");
            final ClassPathResource dialect = new ClassPathResource(getDialect(c.getMetaData().getURL()) + ".sql");
            new ResourceDatabasePopulator(true, true, "UTF-8", schema, dialect.exists() ? dialect : new ClassPathResource("default.sql")).populate(c);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getDialect(String url) {
        final String[] parts = url.split(":");
        assert parts[0].equals("jdbc");
        if (parts.length < 2) {
            throw new IllegalArgumentException("invalid jdbc url");
        }
        return parts[1];
    }
}
