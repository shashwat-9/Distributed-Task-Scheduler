package com.shashwat.scheduler_engine;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class SchedulerEngineApplication {

	private static ConfigurableApplicationContext appContext;

	public static void main(String[] args) {
		appContext = SpringApplication.run(SchedulerEngineApplication.class, args);
	}

	public static ConfigurableApplicationContext getAppContext() {
		return appContext;
	}
}