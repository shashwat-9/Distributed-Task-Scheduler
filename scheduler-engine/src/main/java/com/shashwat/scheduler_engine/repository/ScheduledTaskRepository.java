package com.shashwat.scheduler_engine.repository;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.config.ScheduledTask;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class ScheduledTaskRepository {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public List<ScheduledTask> findScheduledTasks() {
        String sql = "select (1 + 1) as hello";
        jdbcTemplate.query(sql, rs -> {
            System.out.println(rs.getString("hello"));
        });
        return new ArrayList<>(){};
    }
}
