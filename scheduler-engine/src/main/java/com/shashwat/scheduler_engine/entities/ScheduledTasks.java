package com.shashwat.scheduler_engine.entities;

import jakarta.persistence.*;

@Entity
@Table(name = "SCHEDULED_TASK")
public class ScheduledTasks {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "name")
    private String name;
}
