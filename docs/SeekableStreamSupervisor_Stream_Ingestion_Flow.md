# SeekableStreamSupervisor Stream Ingestion Flow

This document provides a comprehensive code flow diagram of the stream ingestion process in Apache Druid's `SeekableStreamSupervisor`, with detailed analysis of Kafka/Kinesis partition assignments.

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Class Hierarchy](#class-hierarchy)
3. [Supervisor Lifecycle](#supervisor-lifecycle)
4. [Main Run Loop Flow](#main-run-loop-flow)
5. [Partition Discovery and Assignment](#partition-discovery-and-assignment)
6. [Task Group Management](#task-group-management)
7. [Task Creation Flow](#task-creation-flow)
8. [Task Execution Flow](#task-execution-flow)
9. [Checkpointing and Publishing](#checkpointing-and-publishing)
10. [Kafka vs Kinesis Differences](#kafka-vs-kinesis-differences)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           OVERLORD (Leader)                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                    SupervisorManager                                  │    │
│  │  ┌─────────────────────────────────────────────────────────────┐    │    │
│  │  │              SeekableStreamSupervisor                        │    │    │
│  │  │  ┌───────────────┐  ┌───────────────┐  ┌───────────────┐    │    │    │
│  │  │  │  KafkaSuperv. │  │ KinesisSuperv │  │ RecordSupplier│    │    │    │
│  │  │  └───────────────┘  └───────────────┘  └───────────────┘    │    │    │
│  │  │                                                               │    │    │
│  │  │  ┌─────────────────────────────────────────────────────┐    │    │    │
│  │  │  │              NoticesQueue                            │    │    │    │
│  │  │  │  [RunNotice, CheckpointNotice, ShutdownNotice, ...]  │    │    │    │
│  │  │  └─────────────────────────────────────────────────────┘    │    │    │
│  │  └─────────────────────────────────────────────────────────────┘    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                         │
│                                    ▼                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                         TaskQueue                                    │    │
│  │    ┌──────────────────────────────────────────────────────────┐     │    │
│  │    │ SeekableStreamIndexTask (Kafka/Kinesis IndexTask)        │     │    │
│  │    └──────────────────────────────────────────────────────────┘     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          MIDDLE MANAGER / PEON                               │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                 SeekableStreamIndexTaskRunner                        │    │
│  │  ┌───────────────────────────────────────────────────────────────┐  │    │
│  │  │  Main Ingestion Loop:                                          │  │    │
│  │  │  - poll() records from stream                                  │  │    │
│  │  │  - parse records → InputRows                                   │  │    │
│  │  │  - add to Appenderator                                         │  │    │
│  │  │  - checkpoint/publish segments                                 │  │    │
│  │  └───────────────────────────────────────────────────────────────┘  │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Class Hierarchy

```
StreamSupervisor (interface)
    └── SeekableStreamSupervisor<PartitionIdType, SequenceOffsetType, RecordType>
            ├── KafkaSupervisor<KafkaTopicPartition, Long, KafkaRecordEntity>
            └── KinesisSupervisor<String, String, KinesisRecordEntity>

SeekableStreamIndexTask<PartitionIdType, SequenceOffsetType, RecordType>
    ├── KafkaIndexTask
    └── KinesisIndexTask

RecordSupplier<PartitionIdType, SequenceOffsetType, RecordType>
    ├── KafkaRecordSupplier
    └── KinesisRecordSupplier
```

### Key Classes and Files

| Class                           | Location                                                              | Purpose                                    |
|---------------------------------|-----------------------------------------------------------------------|--------------------------------------------|
| `SeekableStreamSupervisor`      | `indexing-service/.../supervisor/SeekableStreamSupervisor.java`       | Abstract base class for stream supervisors |
| `KafkaSupervisor`               | `extensions-core/kafka-indexing-service/.../KafkaSupervisor.java`     | Kafka-specific supervisor implementation   |
| `KinesisSupervisor`             | `extensions-core/kinesis-indexing-service/.../KinesisSupervisor.java` | Kinesis-specific supervisor implementation |
| `SeekableStreamIndexTask`       | `indexing-service/.../SeekableStreamIndexTask.java`                   | Abstract base for indexing tasks           |
| `SeekableStreamIndexTaskRunner` | `indexing-service/.../SeekableStreamIndexTaskRunner.java`             | Task runner with main ingestion loop       |
| `RecordSupplier`                | `indexing-service/.../common/RecordSupplier.java`                     | Interface for stream consumer              |

---

## Supervisor Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SUPERVISOR LIFECYCLE                                  │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────┐
     │    start()   │
     └──────┬───────┘
            │
            ▼
     ┌──────────────┐     retry (up to 20 times)
     │   tryInit()  │ ─────────────────────────┐
     └──────┬───────┘                          │
            │ success                          │
            ▼                                  │
     ┌──────────────────────────────────────┐  │
     │  setupRecordSupplier()               │  │
     │  (Creates Kafka/Kinesis consumer)    │◄─┘
     └──────┬───────────────────────────────┘
            │
            ▼
     ┌──────────────────────────────────────┐
     │  Start Notice Processing Thread      │
     │  (exec.submit -> notices.poll())     │
     └──────┬───────────────────────────────┘
            │
            ▼
     ┌──────────────────────────────────────┐
     │  Schedule Periodic RunNotice         │
     │  (scheduledExec.scheduleAtFixedRate) │
     │  Period: ioConfig.period (default 1m)│
     └──────┬───────────────────────────────┘
            │
            ▼
     ┌──────────────────────────────────────┐
     │  Schedule Reporting                  │
     │  (Lag metrics, offset updates)       │
     └──────┬───────────────────────────────┘
            │
            ▼
     ┌──────────────┐
     │   RUNNING    │
     └──────────────┘
```

### Key Initialization Code Path
**File:** `SeekableStreamSupervisor.java:1276-1357`

```java
public void tryInit() {
    recordSupplier = setupRecordSupplier();  // Creates Kafka/Kinesis consumer

    exec.submit(() -> {
        while (!stopped) {
            Notice notice = notices.poll(pollTimeout);
            notice.handle();  // Process RunNotice, CheckpointNotice, etc.
        }
    });

    scheduledExec.scheduleAtFixedRate(
        buildRunTask(),  // Adds RunNotice to queue
        ioConfig.getStartDelay().getMillis(),
        ioConfig.getPeriod().getMillis(),
        TimeUnit.MILLISECONDS
    );
}
```

---

## Main Run Loop Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           runInternal() FLOW                                 │
│                      SeekableStreamSupervisor.java:1733                      │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────────────────────────────┐
     │         RunNotice.handle()           │
     │               │                      │
     │               ▼                      │
     │         runInternal()                │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  1. possiblyRegisterListener()       │ Register TaskRunnerListener
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  2. updatePartitionDataFromStream()  │ ◄── PARTITION DISCOVERY
     │     - recordSupplier.getPartitionIds()│
     │     - Assign partitions to groups    │
     │     - Handle new/expired partitions  │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  3. discoverTasks()                  │ ◄── TASK DISCOVERY
     │     - Find existing tasks            │
     │     - Create TaskGroup for each      │
     │     - Verify checkpoints             │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  4. updateTaskStatus()               │
     │     - Get startTime from tasks       │
     │     - Update task status             │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  5. checkTaskDuration()              │ ◄── TASK LIFECYCLE
     │     - If task exceeded duration      │
     │     - Signal tasks to finish         │
     │     - Move to pendingCompletion      │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  6. checkPendingCompletionTasks()    │
     │     - Monitor publishing tasks       │
     │     - Handle timeouts/failures       │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  7. checkCurrentTaskState()          │
     │     - Verify tasks are healthy       │
     │     - Kill failed tasks              │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  8. checkIfStreamInactiveAndTurn...  │
     │     - Idle detection                 │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  9. createNewTasks()                 │ ◄── TASK CREATION
     │     - Create TaskGroups for groups   │
     │       without active tasks           │
     │     - Create replica tasks           │
     └──────────────────────────────────────┘
```

---

## Partition Discovery and Assignment

### Partition Discovery Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                 updatePartitionDataFromStream()                              │
│                    SeekableStreamSupervisor.java:2849                        │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────────────────────────────┐
     │  recordSupplier.getPartitionIds()    │
     │  (Kafka: KafkaConsumer.partitionsFor)│
     │  (Kinesis: listShards API)           │
     └──────────────┬───────────────────────┘
                    │
                    ▼
     ┌──────────────────────────────────────┐
     │  Get stored metadata offsets         │
     │  indexerMetadataStorageCoordinator   │
     │  .retrieveDataSourceMetadata()       │
     └──────────────┬───────────────────────┘
                    │
                    ▼
     ┌──────────────────────────────────────┐
     │  Filter partitions:                  │
     │  - Remove closed partitions          │
     │  - Remove expired partitions         │
     │  - activePartitionsIdsFromSupplier   │
     └──────────────┬───────────────────────┘
                    │
                    ▼
     ┌──────────────────────────────────────┐
     │  For each active partition:          │
     │  ┌────────────────────────────────┐  │
     │  │ taskGroupId =                  │  │
     │  │   getTaskGroupIdForPartition() │  │
     │  │                                │  │
     │  │ Kafka:  partition % taskCount  │  │
     │  │ Kinesis: index % taskCount     │  │
     │  └────────────────────────────────┘  │
     │                                      │
     │  partitionGroups.put(taskGroupId,    │
     │                      partition)      │
     │  partitionOffsets.put(partition,     │
     │                       NOT_SET)       │
     └──────────────┬───────────────────────┘
                    │
                    ▼
     ┌──────────────────────────────────────┐
     │  If partitions changed:              │
     │  - assignRecordSupplierToPartitionIds│
     │  - Set earlyStopTime for             │
     │    repartition transition            │
     └──────────────────────────────────────┘
```

### Partition Assignment Algorithm

#### Kafka Partition Assignment
**File:** `KafkaSupervisor.java:141-149`

```java
@Override
protected int getTaskGroupIdForPartition(KafkaTopicPartition partitionId) {
    Integer taskCount = spec.getIoConfig().getTaskCount();
    if (partitionId.isMultiTopicPartition()) {
        // Multi-topic: hash based assignment
        return Math.abs(31 * partitionId.topic().hashCode() + partitionId.partition()) % taskCount;
    } else {
        // Single topic: simple modulo
        return partitionId.partition() % taskCount;
    }
}
```

```
Example: 12 partitions, taskCount = 3

Partition 0  → TaskGroup 0   (0 % 3 = 0)
Partition 1  → TaskGroup 1   (1 % 3 = 1)
Partition 2  → TaskGroup 2   (2 % 3 = 2)
Partition 3  → TaskGroup 0   (3 % 3 = 0)
Partition 4  → TaskGroup 1   (4 % 3 = 1)
...
Partition 11 → TaskGroup 2   (11 % 3 = 2)

TaskGroup 0: [P0, P3, P6, P9]
TaskGroup 1: [P1, P4, P7, P10]
TaskGroup 2: [P2, P5, P8, P11]
```

#### Kinesis Shard Assignment
**File:** `KinesisSupervisor.java:213-226`

```java
@Override
protected int getTaskGroupIdForPartition(String partitionId) {
    return getTaskGroupIdForPartitionWithProvidedList(partitionId, partitionIds);
}

private int getTaskGroupIdForPartitionWithProvidedList(String partitionId, List<String> availablePartitions) {
    int index = availablePartitions.indexOf(partitionId);
    if (index < 0) {
        return index;
    }
    return availablePartitions.indexOf(partitionId) % spec.getIoConfig().getTaskCount();
}
```

```
Example: 6 shards, taskCount = 2

Shard List (ordered): [shard-000, shard-001, shard-002, shard-003, shard-004, shard-005]

shard-000 (index 0) → TaskGroup 0   (0 % 2 = 0)
shard-001 (index 1) → TaskGroup 1   (1 % 2 = 1)
shard-002 (index 2) → TaskGroup 0   (2 % 2 = 0)
shard-003 (index 3) → TaskGroup 1   (3 % 2 = 1)
shard-004 (index 4) → TaskGroup 0   (4 % 2 = 0)
shard-005 (index 5) → TaskGroup 1   (5 % 2 = 1)

TaskGroup 0: [shard-000, shard-002, shard-004]
TaskGroup 1: [shard-001, shard-003, shard-005]
```

---

## Task Group Management

### Data Structures

**File:** `SeekableStreamSupervisor.java:845-865`

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        SUPERVISOR DATA STRUCTURES                            │
└─────────────────────────────────────────────────────────────────────────────┘

activelyReadingTaskGroups: ConcurrentHashMap<Integer, TaskGroup>
├── TaskGroup 0
│   ├── groupId: 0
│   ├── startingSequences: {P0→offset1, P3→offset2, P6→offset3}
│   ├── tasks: ConcurrentHashMap<String, TaskData>
│   │   ├── "task-abc-123" → TaskData{status, startTime, currentSequences}
│   │   └── "task-abc-456" → TaskData{status, startTime, currentSequences}
│   ├── checkpointSequences: TreeMap<Integer, Map<PartitionId, Offset>>
│   ├── minimumMessageTime, maximumMessageTime
│   ├── exclusiveStartSequenceNumberPartitions
│   └── baseSequenceName: "seq_kafka_topic_abc123..."
├── TaskGroup 1
│   └── ...
└── TaskGroup 2
    └── ...

pendingCompletionTaskGroups: ConcurrentHashMap<Integer, CopyOnWriteArrayList<TaskGroup>>
├── TaskGroup 0 → [TaskGroup(publishing), TaskGroup(publishing)]
└── ...

partitionGroups: ConcurrentHashMap<Integer, Set<PartitionIdType>>
├── 0 → {P0, P3, P6, P9}
├── 1 → {P1, P4, P7, P10}
└── 2 → {P2, P5, P8, P11}

partitionOffsets: ConcurrentHashMap<PartitionIdType, SequenceOffsetType>
├── P0 → 12345
├── P1 → 67890
└── ...

partitionIds: CopyOnWriteArrayList<PartitionIdType>
└── [P0, P1, P2, P3, P4, P5, P6, P7, P8, P9, P10, P11]
```

### TaskGroup Lifecycle

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         TaskGroup STATE MACHINE                              │
└─────────────────────────────────────────────────────────────────────────────┘

                    ┌─────────────────────┐
                    │  NO TaskGroup       │
                    │  (partitionGroup    │
                    │   exists, but no    │
                    │   TaskGroup yet)    │
                    └──────────┬──────────┘
                               │ createNewTasks()
                               ▼
                    ┌─────────────────────┐
                    │ activelyReading     │ ◄─────────────────────┐
                    │ TaskGroups          │                       │
                    │                     │   New task created    │
                    │ - Tasks are reading │   after previous      │
                    │ - Ingesting data    │   completes           │
                    └──────────┬──────────┘                       │
                               │                                  │
                               │ checkTaskDuration():             │
                               │ task.startTime + taskDuration    │
                               │ has passed                       │
                               ▼                                  │
                    ┌─────────────────────┐                       │
                    │ checkpointTaskGroup │                       │
                    │                     │                       │
                    │ 1. Pause all tasks  │                       │
                    │ 2. Get highest      │                       │
                    │    offsets          │                       │
                    │ 3. Set end offsets  │                       │
                    │ 4. Resume tasks     │                       │
                    └──────────┬──────────┘                       │
                               │                                  │
                               ▼                                  │
                    ┌─────────────────────┐                       │
                    │ pendingCompletion   │                       │
                    │ TaskGroups          │                       │
                    │                     │                       │
                    │ - Tasks are         │                       │
                    │   publishing        │                       │
                    │ - Waiting for       │                       │
                    │   handoff           │                       │
                    └──────────┬──────────┘                       │
                               │                                  │
                               │ Task SUCCESS                     │
                               ▼                                  │
                    ┌─────────────────────┐                       │
                    │ TaskGroup removed   │───────────────────────┘
                    │ from pending        │
                    │ completion          │
                    │                     │
                    │ Offsets updated in  │
                    │ partitionOffsets    │
                    └─────────────────────┘
```

---

## Task Creation Flow

**File:** `SeekableStreamSupervisor.java:3854-3975`

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          createNewTasks() FLOW                               │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────────────────────────────┐
     │  For each groupId in partitionGroups │
     └──────────────┬───────────────────────┘
                    │
                    ▼
     ┌──────────────────────────────────────┐
     │  if (!activelyReadingTaskGroups      │ Check if TaskGroup
     │       .containsKey(groupId))         │ already exists
     └──────────────┬───────────────────────┘
                    │ No TaskGroup exists
                    ▼
     ┌──────────────────────────────────────┐
     │  Generate starting sequences:        │
     │  generateStartingSequencesFor...()   │
     │                                      │
     │  1. Check partitionOffsets           │
     │  2. If NOT_SET, check metadata store │
     │  3. If not in metadata, get from     │
     │     stream (earliest/latest)         │
     └──────────────┬───────────────────────┘
                    │
                    ▼
     ┌──────────────────────────────────────┐
     │  Create new TaskGroup:               │
     │  new TaskGroup(                      │
     │    groupId,                          │
     │    startingSequences,                │
     │    minimumMessageTime,               │
     │    maximumMessageTime,               │
     │    exclusiveStartSequencePartitions  │
     │  )                                   │
     └──────────────┬───────────────────────┘
                    │
                    ▼
     ┌──────────────────────────────────────┐
     │  activelyReadingTaskGroups.put(...)  │
     └──────────────┬───────────────────────┘
                    │
                    ▼
     ┌──────────────────────────────────────┐
     │  For each TaskGroup:                 │
     │  if (replicas > tasks.size())        │
     │     createTasksForGroup(groupId,     │
     │       replicas - tasks.size())       │
     └──────────────┬───────────────────────┘
                    │
                    ▼
     ┌──────────────────────────────────────────────────────────────────┐
     │  createTasksForGroup():                                          │
     │                                                                  │
     │  1. Create SeekableStreamIndexTaskIOConfig                       │
     │     - startPartitions: group.startingSequences                   │
     │     - endPartitions: END_OF_PARTITION marker for each partition  │
     │                                                                  │
     │  2. createIndexTasks() [Kafka/Kinesis specific]                  │
     │     - Creates KafkaIndexTask or KinesisIndexTask                 │
     │     - taskId: random ID with baseSequenceName prefix             │
     │                                                                  │
     │  3. taskQueue.add(indexTask)                                     │
     │     - Task is submitted to Overlord's task queue                 │
     └──────────────────────────────────────────────────────────────────┘
```

---

## Task Execution Flow

### SeekableStreamIndexTaskRunner Main Loop

**File:** `SeekableStreamIndexTaskRunner.java:399-700`

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SeekableStreamIndexTaskRunner.run()                       │
└─────────────────────────────────────────────────────────────────────────────┘

     ┌──────────────────────────────────────┐
     │  1. Initialize                       │
     │     - Create StreamChunkParser       │
     │     - initializeSequences()          │
     │     - Register with ChatHandler      │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  2. Setup RecordSupplier             │
     │     task.newTaskRecordSupplier()     │
     │     - KafkaRecordSupplier            │
     │     - KinesisRecordSupplier          │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  3. Setup Appenderator + Driver      │
     │     - newAppenderator()              │
     │     - newDriver()                    │
     │     - driver.startJob()              │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  4. Restore/Initialize Offsets       │
     │     - currOffsets from metadata      │
     │     - or from startSequenceNumbers   │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  5. Assign & Seek Partitions         │
     │     assignPartitions(recordSupplier) │
     │     seekToStartingSequence()         │
     └──────────────┬───────────────────────┘
                    │
                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MAIN INGESTION LOOP                                  │
│                                                                              │
│   while (stillReading) {                                                     │
│       │                                                                      │
│       ├─► possiblyPause()           // Handle pause/resume from supervisor  │
│       │                                                                      │
│       ├─► if (stopRequested)        // Check if supervisor signaled stop    │
│       │       status = PUBLISHING                                            │
│       │       break                                                          │
│       │                                                                      │
│       ├─► maybePersistAndPublish... // Checkpoint if needed                 │
│       │                                                                      │
│       ├─► getRecords(recordSupplier)                                         │
│       │   └── recordSupplier.poll(timeout)                                   │
│       │       ├── Kafka: KafkaConsumer.poll()                                │
│       │       └── Kinesis: getRecords() API                                  │
│       │                                                                      │
│       └─► for (record : records) {                                           │
│               │                                                              │
│               ├─► verifyRecordInRange()     // Check record is in range     │
│               │                                                              │
│               ├─► parser.parse(record)      // Parse → InputRows            │
│               │                                                              │
│               ├─► driver.add(row, ...)      // Add to Appenderator          │
│               │   │                                                          │
│               │   └─► if (isPushRequired)   // Segment is full              │
│               │           driver.persist()                                   │
│               │                                                              │
│               └─► Update currOffsets[partition] = record.sequenceNumber     │
│           }                                                                  │
│   }                                                                          │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
                    │
                    ▼
     ┌──────────────────────────────────────┐
     │  6. Publishing Phase                 │
     │     status = PUBLISHING              │
     │                                      │
     │     - Persist remaining data         │
     │     - Publish segments               │
     │     - Update metadata                │
     │     - Handoff segments               │
     └──────────────────────────────────────┘
```

---

## Checkpointing and Publishing

### Checkpoint Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         CHECKPOINTING MECHANISM                              │
└─────────────────────────────────────────────────────────────────────────────┘

Supervisor Side (SeekableStreamSupervisor):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

     ┌──────────────────────────────────────┐
     │  checkTaskDuration()                 │
     │  (Task has run for taskDuration)     │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  checkpointTaskGroup(group, true)    │ (finalize=true)
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  1. Pause all tasks:                 │
     │     taskClient.pauseAsync(taskId)    │
     │     → Returns current offsets        │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  2. Compute highest offsets          │
     │     across all replica tasks         │
     │     endOffsets = max(all offsets)    │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  3. Set end offsets and finalize:    │
     │     taskClient.setEndOffsetsAsync(   │
     │       taskId, endOffsets, true)      │
     │     → Task will finish and publish   │
     └──────────────┬───────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  4. Move TaskGroup to                │
     │     pendingCompletionTaskGroups      │
     │                                      │
     │  5. Update partitionOffsets with     │
     │     endOffsets for next TaskGroup    │
     └──────────────────────────────────────┘


Task Side (SeekableStreamIndexTaskRunner):
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

     ┌──────────────────────────────────────┐
     │  POST /pause                         │
     │  → possiblyPause()                   │
     │  → status = PAUSED                   │
     │  → Return currOffsets                │
     └──────────────────────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  POST /offsets/end?finish=true       │
     │  → setEndOffsets(offsets, true)      │
     │  → endOffsets = offsets              │
     │  → Resume reading                    │
     └──────────────────────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  Continue reading until              │
     │  currOffset >= endOffset             │
     │  for all partitions                  │
     └──────────────────────────────────────┘
                    │
     ┌──────────────▼───────────────────────┐
     │  Transition to PUBLISHING:           │
     │  - Persist remaining data            │
     │  - Publish segments                  │
     │  - Checkpoint metadata               │
     │  - Handoff to Historical             │
     └──────────────────────────────────────┘
```

### Metadata Storage

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         METADATA STORAGE                                     │
│                    (druid_dataSource table)                                  │
└─────────────────────────────────────────────────────────────────────────────┘

dataSource: "my_datasource"
supervisorId: "supervisor_my_datasource"

commit_metadata_payload:
{
  "type": "kafka",  // or "kinesis"
  "partitions": {
    "type": "end",
    "stream": "my_topic",
    "partitionSequenceNumberMap": {
      "0": 12345,
      "1": 67890,
      "2": 11111,
      ...
    }
  }
}

This metadata is used to:
1. Resume from the last committed offset on restart
2. Ensure exactly-once semantics with segment metadata
3. Coordinate between supervisor and tasks
```

---

## Kafka vs Kinesis Differences

| Aspect | Kafka | Kinesis |
|--------|-------|---------|
| **Partition ID Type** | `KafkaTopicPartition` (topic + partition int) | `String` (shard ID) |
| **Sequence Type** | `Long` (offset) | `String` (sequence number) |
| **Partition Assignment** | `partition % taskCount` | `indexOf(shard) % taskCount` |
| **Exclusive Start** | Not used (`useExclusiveStartingSequence = false`) | Used (`useExclusiveStartingSequence = true`) |
| **Shard/Partition Expiration** | No (partitions are permanent) | Yes (shards can expire) |
| **Lag Metric** | Record count lag | Time lag (milliseconds) |
| **End of Shard** | N/A | `KinesisSequenceNumber.END_OF_SHARD_MARKER` |
| **RecordSupplier** | `KafkaRecordSupplier` (wraps KafkaConsumer) | `KinesisRecordSupplier` (uses AWS SDK) |

### Kafka-Specific Features

**File:** `KafkaSupervisor.java`

```java
// Multi-topic support with regex patterns
private final Pattern pattern;  // For topic regex matching

// Partition assignment for multi-topic
if (partitionId.isMultiTopicPartition()) {
    return Math.abs(31 * partitionId.topic().hashCode() + partitionId.partition()) % taskCount;
}

// Record lag (offset-based)
@Override
protected Map<KafkaTopicPartition, Long> getPartitionRecordLag() {
    return latestSequenceFromStream - currentOffsets;
}
```

### Kinesis-Specific Features

**File:** `KinesisSupervisor.java`

```java
// Shard expiration handling
@Override
protected boolean supportsPartitionExpiration() {
    return true;
}

@Override
protected boolean isEndOfShard(String seqNum) {
    return KinesisSequenceNumber.END_OF_SHARD_MARKER.equals(seqNum);
}

// Shard rebalancing on expiration
@Override
protected Map<Integer, Set<String>> recomputePartitionGroupsForExpiration(Set<String> availablePartitions) {
    // Redistribute shards evenly across task groups
    List<String> availablePartitionsList = new ArrayList<>(availablePartitions);
    for (String partition : availablePartitions) {
        int newTaskGroupId = indexOf(partition) % taskCount;
        newPartitionGroups.get(newTaskGroupId).add(partition);
    }
    return newPartitionGroups;
}

// Time-based lag (no record count available)
@Override
protected Map<String, Long> getPartitionTimeLag() {
    return currentPartitionTimeLag;  // milliseconds behind latest
}
```

---

## Key Configuration Parameters

### Supervisor IOConfig

| Parameter | Default | Description |
|-----------|---------|-------------|
| `taskCount` | 1 | Number of task groups (parallel readers) |
| `replicas` | 1 | Number of replica tasks per group |
| `taskDuration` | PT1H | How long each task runs before publishing |
| `completionTimeout` | PT30M | Timeout for task completion |
| `startDelay` | PT5S | Delay before first run |
| `period` | PT30S | How often supervisor runs |
| `useEarliestSequenceNumber` | false | Start from earliest or latest |
| `maxAllowedStops` | 1 | Maximum concurrent task group stops |

### Supervisor TuningConfig

| Parameter | Default | Description |
|-----------|---------|-------------|
| `workerThreads` | null | Thread pool size for task communication |
| `chatThreads` | null | Threads for HTTP endpoints |
| `chatRetries` | 8 | Retries for task communication |
| `httpTimeout` | PT10S | HTTP request timeout |
| `shutdownTimeout` | PT80S | Shutdown wait time |
| `repartitionTransitionDuration` | PT2M | Cooldown after partition changes |

---

## Summary Diagram

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                     COMPLETE STREAM INGESTION FLOW                           │
└─────────────────────────────────────────────────────────────────────────────┘

Stream (Kafka/Kinesis)         Overlord                    MiddleManager/Peon
━━━━━━━━━━━━━━━━━━━━━━━━━    ━━━━━━━━━━━━━━━━━━━━━━━━    ━━━━━━━━━━━━━━━━━━━━━━━

┌───────────────────┐
│ Topic/Stream      │
│ ┌───┬───┬───┬───┐ │      ┌──────────────────────┐
│ │P0 │P1 │P2 │P3 │ │─────►│ SeekableStream       │
│ └───┴───┴───┴───┘ │      │ Supervisor           │
└───────────────────┘      │                      │
                           │ 1. getPartitionIds() │
                           │ 2. Assign to groups  │
                           │ 3. Create TaskGroups │
                           └──────────┬───────────┘
                                      │
                                      ▼
                           ┌──────────────────────┐
                           │ createIndexTasks()   │
                           │                      │
                           │ TaskGroup 0:         │
                           │   Task-0a (replica)  │────────┐
                           │   Task-0b (replica)  │────────┤
                           │                      │        │
                           │ TaskGroup 1:         │        │
                           │   Task-1a (replica)  │────────┤
                           └──────────────────────┘        │
                                                          ▼
                                               ┌─────────────────────┐
                                               │ SeekableStream      │
                                               │ IndexTaskRunner     │
                                               │                     │
                                               │ while(reading) {    │
┌───────────────────┐                          │   poll(stream)      │
│ RecordSupplier    │◄─────────────────────────│   parse(records)    │
│ (poll records)    │                          │   add(appenderator) │
└───────────────────┘                          │ }                   │
                                               │                     │
                                               │ publish segments    │──────►  Deep Storage
                                               │ update metadata     │──────►  Metadata Store
                                               │ handoff             │──────►  Historical
                                               └─────────────────────┘
```

---

---

