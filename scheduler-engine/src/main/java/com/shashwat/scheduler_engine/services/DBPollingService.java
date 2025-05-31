package com.shashwat.scheduler_engine.services;

import com.shashwat.scheduler_engine.entities.ScheduledTasks;
import com.shashwat.scheduler_engine.repository.ScheduledTasksRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class DBPollingService {

    private ScheduledTasksRepository scheduledTasksRepository;

    public List<ScheduledTasks> getTasksWithDateAndTime() {
        return scheduledTasksRepository.findAll();
    }

    @Autowired
    public ScheduledTasksRepository setScheduledTasksRepository(ScheduledTasksRepository scheduledTasksRepository) {
        return scheduledTasksRepository;
    }
}
