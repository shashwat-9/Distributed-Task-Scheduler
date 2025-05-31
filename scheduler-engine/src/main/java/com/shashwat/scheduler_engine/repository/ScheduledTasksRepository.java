package com.shashwat.scheduler_engine.repository;

import com.shashwat.scheduler_engine.entities.ScheduledTasks;
import org.springframework.data.jpa.repository.JpaRepository;

public interface ScheduledTasksRepository extends JpaRepository<ScheduledTasks,Integer> {
}
