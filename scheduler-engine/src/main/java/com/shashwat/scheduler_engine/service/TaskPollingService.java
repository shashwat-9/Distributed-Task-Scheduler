package com.shashwat.scheduler_engine.service;

import com.shashwat.scheduler_engine.repository.ScheduledTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.config.ScheduledTask;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class TaskPollingService {

    private static final Logger LOG = LoggerFactory.getLogger(TaskPollingService.class);

    private final ScheduledTaskRepository scheduledTaskRepository;

    @Autowired
    public TaskPollingService(ScheduledTaskRepository scheduledTaskRepository) {
        this.scheduledTaskRepository = scheduledTaskRepository;
    }

    @Scheduled(initialDelay = 5000, fixedDelay = 5000)
    private void taskPolling() {
        LOG.info("Task polling started");
        List<ScheduledTask> scheduledTaskList;
        int recordCount = 0;
        do {
            scheduledTaskList = scheduledTaskRepository.findScheduledTasks();
            recordCount += scheduledTaskList.size();
        }   while (!scheduledTaskList.isEmpty());

        LOG.info("Task polling finished with {} records", recordCount);
    }
}
