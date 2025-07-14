# Distributed Task Scheduler

There are different three microservices in this project:
1. User-Service → Deals with the user Management and task CRUDs
2. Scheduler-Engine → Schedules tasks to different worker Nodes
3. Worker-Node(replicated) → Executes the scheduled task, at the scheduled time.

 - The Scheduler-engine allocates tasks to different worker nodes, via `Kafka`.
 - The results of tasks are published on kafka for the scheduler-engine to update.

Supported Tasks:
1. Scheduled Tasks: The schedule of execution is pre-defined, and the task runs at this schedule.
2. Triggered Tasks: Whenever the task is triggerd, it goes for execution.

All the tasks have the following required information:
1. Image: Choose from the available docker image, where you can run the task
2. script: An array of cmds, to be executed one after another.
 