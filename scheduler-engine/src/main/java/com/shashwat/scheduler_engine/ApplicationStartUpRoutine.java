package com.shashwat.scheduler_engine;

import com.shashwat.scheduler_engine.model.Task;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Component
public class ApplicationStartUpRoutine implements CommandLineRunner {

    @Autowired
    private ThreadPoolExecutor threadPoolExecutor;

    private final BlockingQueue<Task> taskQueue = new LinkedBlockingQueue<>();

    @Override
    public void run(String... args) {
//        startWorkerThreads();
    }

}