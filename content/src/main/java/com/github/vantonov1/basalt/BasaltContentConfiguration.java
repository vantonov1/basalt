package com.github.vantonov1.basalt;

import com.github.vantonov1.basalt.content.impl.ContentDAO;
import com.github.vantonov1.basalt.content.impl.FileContentDAO;
import com.github.vantonov1.basalt.content.impl.MemoryContentDAO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.env.Environment;

import javax.annotation.PostConstruct;

@Configuration
@ComponentScan
public class BasaltContentConfiguration {
    public interface BasaltContentConfigurationCallback {
        String getContentRoot();
    }

    @Autowired
    private Environment environment;

    @Autowired(required = false)
    private BasaltContentConfigurationCallback configurationCallback;

    private String contentRoot;

    @PostConstruct
    private void postConstruct() {
        contentRoot = configurationCallback != null
                ? configurationCallback.getContentRoot()
                : environment.getProperty("content.root");
        if(contentRoot == null) {
            throw new IllegalArgumentException("content.root have to be provided");
        }
    }

    @Bean
    @Lazy
    public ContentDAO getContentDAO() {
        return "mem".equals(contentRoot) ? new MemoryContentDAO() : new FileContentDAO(contentRoot);
    }
}
