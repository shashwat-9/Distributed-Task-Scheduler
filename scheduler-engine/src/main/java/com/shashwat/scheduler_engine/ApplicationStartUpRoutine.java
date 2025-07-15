package com.shashwat.scheduler_engine;

import com.shashwat.scheduler_engine.model.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;

@Component
public class ApplicationStartUpRoutine implements CommandLineRunner {

    private final ThreadPoolExecutor threadPoolExecutor;

    @Autowired
    public ApplicationStartUpRoutine(ThreadPoolExecutor threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    @Override
    public void run(String... args) {

    }

}