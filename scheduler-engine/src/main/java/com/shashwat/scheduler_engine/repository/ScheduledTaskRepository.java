package com.shashwat.scheduler_engine.repository;

import org.springframework.scheduling.config.ScheduledTask;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public class ScheduledTaskRepository {

    public List<ScheduledTask> findScheduledTasks() {
        return null;
    }
}
