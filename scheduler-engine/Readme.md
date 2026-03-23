## Scheduler-Engine

__Language__: Go
__Dependencies__:   
Viber(for configurations),  
Zap(logging),   
lumberjack(for log file rotation),   
confluent-kafka-go(for kafka operations),


Components
1. Kafka Consumer
2. Kafka Producer(transactional)
3. Kubernetes Manager
4. Kubernetes Watcher
5. JobProcessor