package com.shashwat.scheduler_engine.config;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
public class AppConfig {

    private final Environment environment;

    @Autowired
    public AppConfig(Environment environment) {
        this.environment = environment;
    }

    @Bean
    public ThreadPoolExecutor getThreadPoolExecutor() {
        return new ThreadPoolExecutor(Integer.parseInt(Objects.requireNonNull(environment.getProperty("thread.core-pool-size"))),
                Integer.parseInt(Objects.requireNonNull(environment.getProperty("thread.max-pool-size"))),
                Integer.parseInt(Objects.requireNonNull(environment.getProperty("thread.keep-alive-time"))),
                java.util.concurrent.TimeUnit.SECONDS,
                new java.util.concurrent.LinkedBlockingQueue<>());
    }
}