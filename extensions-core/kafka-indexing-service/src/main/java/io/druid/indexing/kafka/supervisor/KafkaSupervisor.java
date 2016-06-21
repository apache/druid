/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexing.kafka.supervisor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.MoreExecutors;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import io.druid.concurrent.Execs;
import io.druid.indexing.common.TaskInfoProvider;
import io.druid.indexing.common.TaskLocation;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.indexing.common.task.TaskResource;
import io.druid.indexing.kafka.KafkaDataSourceMetadata;
import io.druid.indexing.kafka.KafkaIOConfig;
import io.druid.indexing.kafka.KafkaIndexTask;
import io.druid.indexing.kafka.KafkaIndexTaskClient;
import io.druid.indexing.kafka.KafkaIndexTaskClientFactory;
import io.druid.indexing.kafka.KafkaPartitions;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.TaskQueue;
import io.druid.indexing.overlord.TaskRunner;
import io.druid.indexing.overlord.TaskRunnerListener;
import io.druid.indexing.overlord.TaskRunnerWorkItem;
import io.druid.indexing.overlord.TaskStorage;
import io.druid.indexing.overlord.supervisor.Supervisor;
import io.druid.indexing.overlord.supervisor.SupervisorReport;
import io.druid.metadata.EntryExistsException;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Supervisor responsible for managing the KafkaIndexTasks for a single dataSource. At a high level, the class accepts a
 * {@link KafkaSupervisorSpec} which includes the Kafka topic and configuration as well as an ingestion spec which will
 * be used to generate the indexing tasks. The run loop periodically refreshes its view of the Kafka topic's partitions
 * and the list of running indexing tasks and ensures that all partitions are being read from and that there are enough
 * tasks to satisfy the desired number of replicas. As tasks complete, new tasks are queued to process the next range of
 * Kafka offsets.
 */
public class KafkaSupervisor implements Supervisor
{
  private static final EmittingLogger log = new EmittingLogger(KafkaSupervisor.class);
  private static final Random RANDOM = new Random();
  private static final long MAX_RUN_FREQUENCY_MILLIS = 1000; // prevent us from running too often in response to events

  // Internal data structures
  // --------------------------------------------------------

  /**
   * A TaskGroup is the main data structure used by KafkaSupervisor to organize and monitor Kafka partitions and
   * indexing tasks. All the tasks in a TaskGroup should always be doing the same thing (reading the same partitions and
   * starting from the same offset) and if [replicas] is configured to be 1, a TaskGroup will contain a single task (the
   * exception being if the supervisor started up and discovered and adopted some already running tasks). At any given
   * time, there should only be up to a maximum of [taskCount] actively-reading task groups (tracked in the [taskGroups]
   * map) + zero or more pending-completion task groups (tracked in [pendingCompletionTaskGroups]).
   */
  private class TaskGroup
  {
    // This specifies the partitions and starting offsets for this task group. It is set on group creation from the data
    // in [partitionGroups] and never changes during the lifetime of this task group, which will live until a task in
    // this task group has completed successfully, at which point this will be destroyed and a new task group will be
    // created with new starting offsets. This allows us to create replacement tasks for failed tasks that process the
    // same offsets, even if the values in [partitionGroups] has been changed.
    final Map<Integer, Long> partitionOffsets;

    final Map<String, TaskData> tasks = new HashMap<>();
    final Optional<DateTime> minimumMessageTime;
    DateTime completionTimeout; // is set after signalTasksToFinish(); if not done by timeout, take corrective action

    public TaskGroup(Map<Integer, Long> partitionOffsets, Optional<DateTime> minimumMessageTime)
    {
      this.partitionOffsets = partitionOffsets;
      this.minimumMessageTime = minimumMessageTime;
    }
  }

  private class TaskData
  {
    TaskStatus status;
    DateTime startTime;
  }

  // Map<{group ID}, {actively reading task group}>; see documentation for TaskGroup class
  private final HashMap<Integer, TaskGroup> taskGroups = new HashMap<>();

  // After telling a taskGroup to stop reading and begin publishing a segment, it is moved from [taskGroups] to here so
  // we can monitor its status while we queue new tasks to read the next range of offsets. This is a list since we could
  // have multiple sets of tasks publishing at once if time-to-publish > taskDuration.
  // Map<{group ID}, List<{pending completion task groups}>>
  private final HashMap<Integer, List<TaskGroup>> pendingCompletionTaskGroups = new HashMap<>();

  // The starting offset for a new partition in [partitionGroups] is initially set to null. When a new task group
  // is created and is assigned partitions, if the offset in [partitionGroups] is null it will take the starting
  // offset value from the metadata store, and if it can't find it there, from Kafka. Once a task begins
  // publishing, the offset in partitionGroups will be updated to the ending offset of the publishing-but-not-yet-
  // completed task, which will cause the next set of tasks to begin reading from where the previous task left
  // off. If that previous task now fails, we will set the offset in [partitionGroups] back to null which will
  // cause successive tasks to again grab their starting offset from metadata store. This mechanism allows us to
  // start up successive tasks without waiting for the previous tasks to succeed and still be able to handle task
  // failures during publishing.
  // Map<{group ID}, Map<{partition ID}, {startingOffset}>>
  private Map<Integer, Map<Integer, Long>> partitionGroups = new HashMap<>();
  // --------------------------------------------------------

  private final TaskStorage taskStorage;
  private final TaskMaster taskMaster;
  private final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator;
  private final KafkaIndexTaskClient taskClient;
  private final ObjectMapper sortingMapper;
  private final KafkaSupervisorSpec spec;
  private final String dataSource;
  private final KafkaSupervisorIOConfig ioConfig;
  private final String supervisorId;
  private final TaskInfoProvider taskInfoProvider;

  private final ExecutorService exec;
  private final ScheduledExecutorService scheduledExec;
  private final BlockingQueue<Notice> notices = new LinkedBlockingDeque<>();
  private final Object stopLock = new Object();

  private boolean listenerRegistered = false;
  private long lastRunTime;

  private volatile DateTime firstRunTime;
  private volatile KafkaConsumer consumer;
  private volatile boolean started = false;
  private volatile boolean stopped = false;

  public KafkaSupervisor(
      final TaskStorage taskStorage,
      final TaskMaster taskMaster,
      final IndexerMetadataStorageCoordinator indexerMetadataStorageCoordinator,
      final KafkaIndexTaskClientFactory taskClientFactory,
      final ObjectMapper mapper,
      final KafkaSupervisorSpec spec
  )
  {
    this.taskStorage = taskStorage;
    this.taskMaster = taskMaster;
    this.indexerMetadataStorageCoordinator = indexerMetadataStorageCoordinator;
    this.sortingMapper = mapper.copy().configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    this.spec = spec;

    this.dataSource = spec.getDataSchema().getDataSource();
    this.ioConfig = spec.getIoConfig();
    this.supervisorId = String.format("KafkaSupervisor-%s", dataSource);
    this.exec = Execs.singleThreaded(supervisorId + "-%d");
    this.scheduledExec = Execs.scheduledSingleThreaded(supervisorId + "-Scheduler-%d");

    this.taskInfoProvider = new TaskInfoProvider()
    {
      @Override
      public TaskLocation getTaskLocation(final String id)
      {
        Preconditions.checkNotNull(id, "id");
        Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
        if (taskRunner.isPresent()) {
          Optional<? extends TaskRunnerWorkItem> item = Iterables.tryFind(
              taskRunner.get().getRunningTasks(), new Predicate<TaskRunnerWorkItem>()
              {
                @Override
                public boolean apply(TaskRunnerWorkItem taskRunnerWorkItem)
                {
                  return id.equals(taskRunnerWorkItem.getTaskId());
                }
              }
          );

          if (item.isPresent()) {
            return item.get().getLocation();
          }
        } else {
          log.error("Failed to get task runner because I'm not the leader!");
        }

        return TaskLocation.unknown();
      }

      @Override
      public Optional<TaskStatus> getTaskStatus(String id)
      {
        return taskStorage.getStatus(id);
      }
    };

    this.taskClient = taskClientFactory.build(taskInfoProvider);
  }

  @Override
  public void start()
  {
    Preconditions.checkState(!started, "already started");
    Preconditions.checkState(!exec.isShutdown(), "already stopped");

    try {
      consumer = getKafkaConsumer();

      exec.submit(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                while (!Thread.currentThread().isInterrupted()) {
                  final Notice notice = notices.take();

                  try {
                    notice.handle();
                  }
                  catch (Exception e) {
                    log.makeAlert(e, "KafkaSupervisor[%s] failed to handle notice", dataSource)
                       .addData("noticeClass", notice.getClass().getSimpleName())
                       .emit();
                  }
                }
              }
              catch (InterruptedException e) {
                log.info("KafkaSupervisor[%s] interrupted, exiting", dataSource);
              }
            }
          }
      );
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception starting KafkaSupervisor[%s]", dataSource)
         .emit();
      throw Throwables.propagate(e);
    }

    firstRunTime = DateTime.now().plus(ioConfig.getStartDelay());
    scheduledExec.scheduleAtFixedRate(
        buildRunTask(),
        ioConfig.getStartDelay().getMillis(),
        Math.max(ioConfig.getPeriod().getMillis(), MAX_RUN_FREQUENCY_MILLIS),
        TimeUnit.MILLISECONDS
    );

    started = true;
    log.info("Started KafkaSupervisor[%s], first run in [%s]", dataSource, ioConfig.getStartDelay());
  }

  @Override
  public void stop(boolean stopGracefully)
  {
    Preconditions.checkState(started, "not started");

    log.info("Beginning shutdown of KafkaSupervisor[%s]", dataSource);

    try {
      scheduledExec.shutdownNow(); // stop recurring executions

      Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
      if (taskRunner.isPresent()) {
        taskRunner.get().unregisterListener(supervisorId);
      }

      // Stopping gracefully will synchronize the end offsets of the tasks and signal them to publish, and will block
      // until the tasks have acknowledged or timed out. We want this behavior when we're explicitly shut down through
      // the API, but if we shut down for other reasons (e.g. we lose leadership) we want to just stop and leave the
      // tasks as they are.
      if (stopGracefully) {
        log.info("Stopping gracefully, signalling managed tasks to complete and publish");
        synchronized (stopLock) {
          notices.add(new ShutdownNotice());
          while (!stopped) {
            stopLock.wait();
          }
        }
        log.info("Shutdown notice handled");
      }

      exec.shutdownNow();
      consumer.close();
      started = false;

      log.info("KafkaSupervisor[%s] has stopped", dataSource);
    }
    catch (Exception e) {
      log.makeAlert(e, "Exception stopping KafkaSupervisor[%s]", dataSource)
         .emit();
    }
  }

  @Override
  public SupervisorReport getStatus()
  {
    return generateReport(true);
  }

  public void possiblyRegisterListener()
  {
    // getTaskRunner() sometimes fails if the task queue is still being initialized so retry later until we succeed

    if (listenerRegistered) {
      return;
    }

    Optional<TaskRunner> taskRunner = taskMaster.getTaskRunner();
    if (taskRunner.isPresent()) {
      taskRunner.get().registerListener(
          new TaskRunnerListener()
          {
            @Override
            public String getListenerId()
            {
              return supervisorId;
            }

            @Override
            public void locationChanged(final String taskId, final TaskLocation newLocation)
            {
              // do nothing
            }

            @Override
            public void statusChanged(String taskId, TaskStatus status)
            {
              notices.add(new RunNotice());
            }
          }, MoreExecutors.sameThreadExecutor()
      );

      listenerRegistered = true;
    }
  }

  private interface Notice
  {
    void handle();
  }

  private class RunNotice implements Notice
  {
    @Override
    public void handle()
    {
      long nowTime = System.currentTimeMillis();
      if (nowTime - lastRunTime < MAX_RUN_FREQUENCY_MILLIS) {
        return;
      }
      lastRunTime = nowTime;

      runInternal();
    }
  }

  private class ShutdownNotice implements Notice
  {
    @Override
    public void handle()
    {
      // Prepare for shutdown by 1) killing all tasks that haven't been assigned to a worker yet, and 2) causing all
      // running tasks to begin publishing by setting their startTime to a very long time ago so that the logic in
      // checkTaskDuration() will be triggered. This is better than just telling these tasks to publish whatever they
      // have, as replicas that are supposed to publish the same segment may not have read the same set of offsets.
      for (TaskGroup taskGroup : taskGroups.values()) {
        for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
          if (taskInfoProvider.getTaskLocation(entry.getKey()).equals(TaskLocation.unknown())) {
            killTask(entry.getKey());
          } else {
            entry.getValue().startTime = new DateTime(0);
          }
        }
      }

      checkTaskDuration();

      synchronized (stopLock) {
        stopped = true;
        stopLock.notifyAll();
      }
    }
  }

  @VisibleForTesting
  void runInternal()
  {
    possiblyRegisterListener();
    updatePartitionDataFromKafka();
    discoverTasks();
    updateTaskStatus();
    checkTaskDuration();
    checkPendingCompletionTasks();
    checkCurrentTaskState();
    createNewTasks();

    if (log.isDebugEnabled()) {
      log.debug(generateReport(true).toString());
    } else {
      log.info(generateReport(false).toString());
    }
  }

  @VisibleForTesting
  String generateSequenceName(int groupId)
  {
    StringBuilder sb = new StringBuilder();
    Map<Integer, Long> startPartitions = taskGroups.get(groupId).partitionOffsets;

    for (Map.Entry<Integer, Long> entry : startPartitions.entrySet()) {
      sb.append(String.format("+%d(%d)", entry.getKey(), entry.getValue()));
    }
    String partitionOffsetStr = sb.toString().substring(1);

    Optional<DateTime> minimumMessageTime = taskGroups.get(groupId).minimumMessageTime;
    String minMsgTimeStr = (minimumMessageTime.isPresent() ? String.valueOf(minimumMessageTime.get().getMillis()) : "");

    String dataSchema, tuningConfig;
    try {
      dataSchema = sortingMapper.writeValueAsString(spec.getDataSchema());
      tuningConfig = sortingMapper.writeValueAsString(spec.getTuningConfig());
    }
    catch (JsonProcessingException e) {
      throw Throwables.propagate(e);
    }

    String hashCode = DigestUtils.sha1Hex(dataSchema + tuningConfig + partitionOffsetStr + minMsgTimeStr)
                                 .substring(0, 15);

    return Joiner.on("_").join("index_kafka", dataSource, hashCode);
  }

  private static String getRandomId()
  {
    final StringBuilder suffix = new StringBuilder(8);
    for (int i = 0; i < Ints.BYTES * 2; ++i) {
      suffix.append((char) ('a' + ((RANDOM.nextInt() >>> (i * 4)) & 0x0F)));
    }
    return suffix.toString();
  }

  private KafkaConsumer<byte[], byte[]> getKafkaConsumer()
  {
    final Properties props = new Properties();
    props.putAll(ioConfig.getConsumerProperties());

    props.setProperty("enable.auto.commit", "false");
    props.setProperty("metadata.max.age.ms", "10000");
    props.setProperty("group.id", String.format("kafka-supervisor-%s", getRandomId()));

    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      return new KafkaConsumer<>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer());
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  private void updatePartitionDataFromKafka()
  {
    Map<String, List<PartitionInfo>> topics;
    try {
      topics = consumer.listTopics(); // updates the consumer's list of partitions from the brokers
    }
    catch (Exception e) { // calls to the consumer throw NPEs when the broker doesn't respond
      log.warn(
          e,
          "Unable to get partition data from Kafka for brokers [%s], are the brokers up?",
          ioConfig.getConsumerProperties().get(KafkaSupervisorIOConfig.BOOTSTRAP_SERVERS_KEY)
      );
      return;
    }

    List<PartitionInfo> partitions = topics.get(ioConfig.getTopic());
    int numPartitions = (partitions != null ? partitions.size() : 0);

    log.debug("Found [%d] Kafka partitions for topic [%s]", numPartitions, ioConfig.getTopic());

    for (int partition = 0; partition < numPartitions; partition++) {
      int taskGroupId = getTaskGroupIdForPartition(partition);

      if (partitionGroups.get(taskGroupId) == null) {
        partitionGroups.put(taskGroupId, new HashMap<Integer, Long>());
      }

      Map<Integer, Long> partitionMap = partitionGroups.get(taskGroupId);
      if (!partitionMap.containsKey(partition)) {
        log.info(
            "New partition [%d] discovered for topic [%s], adding to task group [%d]",
            partition,
            ioConfig.getTopic(),
            taskGroupId
        );

        // The starting offset for a new partition in [partitionGroups] is initially set to null; when a new task group
        // is created and is assigned partitions, if the offset in [partitionGroups] is null it will take the starting
        // offset value from the metadata store, and if it can't find it there, from Kafka. Once a task begins
        // publishing, the offset in partitionGroups will be updated to the ending offset of the publishing-but-not-yet-
        // completed task, which will cause the next set of tasks to begin reading from where the previous task left
        // off. If that previous task now fails, we will set the offset in [partitionGroups] back to null which will
        // cause successive tasks to again grab their starting offset from metadata store. This mechanism allows us to
        // start up successive tasks without waiting for the previous tasks to succeed and still be able to handle task
        // failures during publishing.
        partitionMap.put(partition, null);
      }
    }
  }

  private void discoverTasks()
  {
    int taskCount = 0;
    List<Task> tasks = taskStorage.getActiveTasks();

    for (Task task : tasks) {
      if (task instanceof KafkaIndexTask && dataSource.equals(task.getDataSource())) {
        taskCount++;
        KafkaIndexTask kafkaTask = (KafkaIndexTask) task;
        String taskId = task.getId();

        // Determine which task group this task belongs to based on one of the partitions handled by this task. If we
        // later determine that this task is actively reading, we will make sure that it matches our current partition
        // allocation (getTaskGroupIdForPartition(partition) should return the same value for every partition being read
        // by this task) and kill it if it is not compatible. If the task is instead found to be in the publishing
        // state, we will permit it to complete even if it doesn't match our current partition allocation to support
        // seamless schema migration.

        Iterator<Integer> it = kafkaTask.getIOConfig().getStartPartitions().getPartitionOffsetMap().keySet().iterator();
        Integer taskGroupId = (it.hasNext() ? getTaskGroupIdForPartition(it.next()) : null);

        if (taskGroupId != null) {
          // check to see if we already know about this task, either in [taskGroups] or in [pendingCompletionTaskGroups]
          // and if not add it to taskGroups or pendingCompletionTaskGroups (if status = PUBLISHING)
          TaskGroup taskGroup = taskGroups.get(taskGroupId);
          if (!isTaskInPendingCompletionGroups(taskId) && (taskGroup == null || !taskGroup.tasks.containsKey(taskId))) {
            Optional<KafkaIndexTask.Status> status = getTaskStatus(taskId);
            if (status.isPresent() && status.get() == KafkaIndexTask.Status.PUBLISHING) {
              addDiscoveredTaskToPendingCompletionTaskGroups(
                  taskGroupId,
                  taskId,
                  kafkaTask.getIOConfig()
                           .getStartPartitions()
                           .getPartitionOffsetMap()
              );

              // update partitionGroups with the publishing task's offsets (if they are greater than what is existing)
              // so that the next tasks will start reading from where this task left off
              Map<Integer, Long> publishingTaskCurrentOffsets = getCurrentOffsets(taskId, true);
              for (Map.Entry<Integer, Long> entry : publishingTaskCurrentOffsets.entrySet()) {
                Integer partition = entry.getKey();
                Long offset = entry.getValue();
                Map<Integer, Long> partitionOffsets = partitionGroups.get(getTaskGroupIdForPartition(partition));
                if (partitionOffsets.get(partition) == null || partitionOffsets.get(partition) < offset) {
                  partitionOffsets.put(partition, offset);
                }
              }

            } else {
              for (Integer partition : kafkaTask.getIOConfig().getStartPartitions().getPartitionOffsetMap().keySet()) {
                if (!taskGroupId.equals(getTaskGroupIdForPartition(partition))) {
                  log.warn("Stopping task [%s] which does not match the expected partition allocation", taskId);
                  stopTask(taskId, false);
                  taskGroupId = null;
                  break;
                }
              }

              if (taskGroupId == null) {
                continue;
              }

              if (!taskGroups.containsKey(taskGroupId)) {
                log.debug("Creating new task group [%d]", taskGroupId);
                taskGroups.put(
                    taskGroupId,
                    new TaskGroup(
                        kafkaTask.getIOConfig().getStartPartitions().getPartitionOffsetMap(),
                        kafkaTask.getIOConfig().getMinimumMessageTime()
                    )
                );
              }

              if (!isTaskCurrent(taskGroupId, taskId)) {
                log.info("Stopping task [%s] which does not match the expected parameters and ingestion spec", taskId);
                stopTask(taskId, false);
              } else {
                taskGroups.get(taskGroupId).tasks.put(taskId, new TaskData());
              }
            }
          }
        }
      }
    }

    log.debug("Found [%d] Kafka indexing tasks for dataSource [%s]", taskCount, dataSource);
  }

  private void addDiscoveredTaskToPendingCompletionTaskGroups(
      int groupId,
      String taskId,
      Map<Integer, Long> startingPartitions
  )
  {
    if (!pendingCompletionTaskGroups.containsKey(groupId)) {
      pendingCompletionTaskGroups.put(groupId, Lists.<TaskGroup>newArrayList());
    }

    List<TaskGroup> taskGroupList = pendingCompletionTaskGroups.get(groupId);
    for (TaskGroup taskGroup : taskGroupList) {
      if (taskGroup.partitionOffsets.equals(startingPartitions)) {
        if (!taskGroup.tasks.containsKey(taskId)) {
          log.info("Adding discovered task [%s] to existing pending task group", taskId);
          taskGroup.tasks.put(taskId, new TaskData());
        }
        return;
      }
    }

    log.info("Creating new pending completion task group for discovered task [%s]", taskId);

    // reading the minimumMessageTime from the publishing task and setting it here is not necessary as this task cannot
    // change to a state where it will read any more events
    TaskGroup newTaskGroup = new TaskGroup(startingPartitions, Optional.<DateTime>absent());

    newTaskGroup.tasks.put(taskId, new TaskData());
    newTaskGroup.completionTimeout = DateTime.now().plus(ioConfig.getCompletionTimeout());

    taskGroupList.add(newTaskGroup);
  }

  private void updateTaskStatus()
  {
    // update status (and startTime if unknown) of current tasks in taskGroups
    for (TaskGroup group : taskGroups.values()) {
      for (Map.Entry<String, TaskData> entry : group.tasks.entrySet()) {
        String taskId = entry.getKey();
        TaskData taskData = entry.getValue();

        if (taskData.startTime == null) {
          try {
            taskData.startTime = getTaskStartTime(taskId);
            if (taskData.startTime != null) {
              long millisRemaining = ioConfig.getTaskDuration().getMillis() - (System.currentTimeMillis()
                                                                               - taskData.startTime.getMillis());
              if (millisRemaining > 0) {
                scheduledExec.schedule(
                    buildRunTask(),
                    millisRemaining + MAX_RUN_FREQUENCY_MILLIS,
                    TimeUnit.MILLISECONDS
                );
              }
            }
          }
          catch (Exception e) {
            log.warn(e, "Task [%s] failed to return start time, killing task", taskId);
            killTask(taskId);
          }
        }

        taskData.status = taskStorage.getStatus(taskId).get();
      }
    }

    // update status of pending completion tasks in pendingCompletionTaskGroups
    for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
      for (TaskGroup group : taskGroups) {
        for (Map.Entry<String, TaskData> entry : group.tasks.entrySet()) {
          entry.getValue().status = taskStorage.getStatus(entry.getKey()).get();
        }
      }
    }
  }

  private void checkTaskDuration()
  {
    Iterator<Map.Entry<Integer, TaskGroup>> i = taskGroups.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry<Integer, TaskGroup> groupEntry = i.next();
      Integer groupId = groupEntry.getKey();
      TaskGroup group = groupEntry.getValue();

      // find the longest running task from this group
      DateTime earliestTaskStart = DateTime.now();
      for (TaskData taskData : group.tasks.values()) {
        if (earliestTaskStart.isAfter(taskData.startTime)) {
          earliestTaskStart = taskData.startTime;
        }
      }

      // if this task has run longer than the configured duration, signal all tasks in the group to persist
      if (earliestTaskStart.plus(ioConfig.getTaskDuration()).isBeforeNow()) {
        log.info("Task group [%d] has run for [%s]", groupId, ioConfig.getTaskDuration());
        Map<Integer, Long> endOffsets = signalTasksToFinish(groupId);

        // set a timeout and put this group in pendingCompletionTaskGroups so that it can be monitored for completion
        group.completionTimeout = DateTime.now().plus(ioConfig.getCompletionTimeout());
        if (!pendingCompletionTaskGroups.containsKey(groupId)) {
          pendingCompletionTaskGroups.put(groupId, Lists.<TaskGroup>newArrayList());
        }
        pendingCompletionTaskGroups.get(groupId).add(group);

        // if we know what the endOffsets are going to be from talking to the tasks, set them as the next startOffsets
        if (endOffsets != null) {
          for (Map.Entry<Integer, Long> entry : endOffsets.entrySet()) {
            partitionGroups.get(groupId).put(entry.getKey(), entry.getValue());
          }
        }

        // remove this task group from the list of current task groups now that it has been handled
        i.remove();
      }
    }
  }

  private Map<Integer, Long> signalTasksToFinish(int groupId)
  {
    TaskGroup taskGroup = taskGroups.get(groupId);

    // 1) Pause running tasks and build a map of the highest offset read by any task in the group for each partition
    Map<Integer, Long> endOffsets = new HashMap<>();
    Iterator<Map.Entry<String, TaskData>> i = taskGroup.tasks.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry<String, TaskData> taskEntry = i.next();
      String taskId = taskEntry.getKey();
      TaskData task = taskEntry.getValue();

      if (task.status.isSuccess()) {
        // If any task in this group has already completed, stop the rest of the tasks in the group and return.
        // This will cause us to create a new set of tasks next cycle that will start from the offsets in
        // metadata store (which will have advanced if we succeeded in publishing and will remain the same if publishing
        // failed and we need to re-ingest)
        stopTasksInGroup(taskGroup);
        return null;
      }

      if (task.status.isRunnable()) {
        if (taskInfoProvider.getTaskLocation(taskId).equals(TaskLocation.unknown())) {
          log.info("Killing task [%s] which hasn't been assigned to a worker", taskId);
          killTask(taskId);
          i.remove();
        } else {
          Map<Integer, Long> currentOffsets;
          try {
            currentOffsets = taskClient.pause(taskId); // pause task and get offsets
          }
          catch (Exception e) {
            log.warn(e, "Task [%s] failed to respond to [pause] in a timely manner, killing task", taskId);
            killTask(taskId);
            i.remove();
            continue;
          }

          for (Map.Entry<Integer, Long> offset : currentOffsets.entrySet()) {
            if (!endOffsets.containsKey(offset.getKey())
                || endOffsets.get(offset.getKey()).compareTo(offset.getValue()) < 0) {
              endOffsets.put(offset.getKey(), offset.getValue());
            }
          }
        }
      }
    }

    // 2) Set the end offsets for each task to the values from step 1 and resume the tasks. All the tasks should
    //    finish reading and start publishing within a short period of time, depending on how in sync the tasks were.
    log.info("Setting endOffsets for tasks in taskGroup [%d] to %s and resuming", groupId, endOffsets);
    i = taskGroup.tasks.entrySet().iterator();
    while (i.hasNext()) {
      Map.Entry<String, TaskData> taskEntry = i.next();
      String taskId = taskEntry.getKey();
      TaskData task = taskEntry.getValue();

      if (task.status.isRunnable()) {
        try {
          taskClient.setEndOffsets(taskId, endOffsets, true);
        }
        catch (Exception e) {
          log.warn(e, "Task [%s] failed to respond to [set end offsets] in a timely manner, killing task", taskId);
          killTask(taskId);
          i.remove();
        }
      }
    }

    // 3) Return the ending offsets so we can start the next set of tasks from where these tasks ended while the current
    //    set of tasks are publishing.
    return endOffsets;
  }

  /**
   * Monitors [pendingCompletionTaskGroups] for tasks that have completed. If any task in a task group has completed, we
   * can safely stop the rest of the tasks in that group. If a task group has exceeded its publishing timeout, then
   * we need to stop all tasks in not only that task group but also 1) any subsequent task group that is also pending
   * completion and 2) the current task group that is running, because the assumption that we have handled up to the
   * starting offset for subsequent task groups is no longer valid, and subsequent tasks would fail as soon as they
   * attempted to publish because of the contiguous range consistency check.
   */
  private void checkPendingCompletionTasks()
  {
    for (Map.Entry<Integer, List<TaskGroup>> pendingGroupList : pendingCompletionTaskGroups.entrySet()) {

      boolean stopTasksInTaskGroup = false;
      Integer groupId = pendingGroupList.getKey();
      Iterator<TaskGroup> iTaskGroup = pendingGroupList.getValue().iterator();
      while (iTaskGroup.hasNext()) {
        boolean foundSuccess = false, entireTaskGroupFailed = false;
        TaskGroup group = iTaskGroup.next();

        if (stopTasksInTaskGroup) {
          // One of the earlier groups that was handling the same partition set timed out before the segments were
          // published so stop any additional groups handling the same partition set that are pending completion.
          stopTasksInGroup(group);
          iTaskGroup.remove();
          continue;
        }

        Iterator<Map.Entry<String, TaskData>> iTask = group.tasks.entrySet().iterator();
        while (iTask.hasNext()) {
          Map.Entry<String, TaskData> task = iTask.next();

          if (task.getValue().status.isFailure()) {
            iTask.remove(); // remove failed task
            if (group.tasks.isEmpty()) {
              // if all tasks in the group have failed, just nuke all task groups with this partition set and restart
              entireTaskGroupFailed = true;
              break;
            }
          }

          if (task.getValue().status.isSuccess()) {
            // If one of the pending completion tasks was successful, stop the rest of the tasks in the group as
            // we no longer need them to publish their segment.
            log.info("Task [%s] completed successfully, stopping tasks %s", task.getKey(), group.tasks.keySet());
            stopTasksInGroup(group);
            foundSuccess = true;
            iTaskGroup.remove(); // remove the TaskGroup from the list of pending completion task groups
            break; // skip iterating the rest of the tasks in this group as they've all been stopped now
          }
        }

        if ((!foundSuccess && group.completionTimeout.isBeforeNow()) || entireTaskGroupFailed) {
          if (entireTaskGroupFailed) {
            log.warn("All tasks in group [%d] failed to publish, killing all tasks for these partitions", groupId);
          } else {
            log.makeAlert(
                "No task in [%s] succeeded before the completion timeout elapsed [%s]!",
                group.tasks.keySet(),
                ioConfig.getCompletionTimeout()
            ).emit();
          }

          // reset partitions offsets for this task group so that they will be re-read from metadata storage
          partitionGroups.remove(groupId);

          // stop all the tasks in this pending completion group
          stopTasksInGroup(group);

          // set a flag so the other pending completion groups for this set of partitions will also stop
          stopTasksInTaskGroup = true;

          // stop all the tasks in the currently reading task group and remove the bad task group
          stopTasksInGroup(taskGroups.remove(groupId));

          iTaskGroup.remove();
        }
      }
    }
  }

  private void checkCurrentTaskState()
  {
    Iterator<Map.Entry<Integer, TaskGroup>> iTaskGroups = taskGroups.entrySet().iterator();
    while (iTaskGroups.hasNext()) {
      Map.Entry<Integer, TaskGroup> taskGroupEntry = iTaskGroups.next();
      Integer groupId = taskGroupEntry.getKey();
      TaskGroup taskGroup = taskGroupEntry.getValue();

      // Iterate the list of known tasks in this group and:
      //   1) Kill any tasks which are not "current" (have the partitions, starting offsets, and minimumMessageTime
      //      (if applicable) in [taskGroups])
      //   2) Remove any tasks that have failed from the list
      //   3) If any task completed successfully, stop all the tasks in this group and move to the next group

      log.debug("Task group [%d] pre-pruning: %s", groupId, taskGroup.tasks.keySet());

      Iterator<Map.Entry<String, TaskData>> iTasks = taskGroup.tasks.entrySet().iterator();
      while (iTasks.hasNext()) {
        Map.Entry<String, TaskData> task = iTasks.next();
        String taskId = task.getKey();
        TaskData taskData = task.getValue();

        // stop and remove bad tasks from the task group
        if (!isTaskCurrent(groupId, taskId)) {
          log.info("Stopping task [%s] which does not match the expected offset range and ingestion spec", taskId);
          stopTask(taskId, false);
          iTasks.remove();
          continue;
        }

        // remove failed tasks
        if (taskData.status.isFailure()) {
          iTasks.remove();
          continue;
        }

        // check for successful tasks, and if we find one, stop all tasks in the group and remove the group so it can
        // be recreated with the next set of offsets
        if (taskData.status.isSuccess()) {
          stopTasksInGroup(taskGroup);
          iTaskGroups.remove();
          break;
        }
      }
      log.debug("Task group [%d] post-pruning: %s", groupId, taskGroup.tasks.keySet());
    }
  }

  void createNewTasks()
  {
    // check that there is a current task group for each group of partitions in [partitionGroups]
    for (Integer groupId : partitionGroups.keySet()) {
      if (!taskGroups.containsKey(groupId)) {
        log.info("Creating new task group [%d] for partitions %s", groupId, partitionGroups.get(groupId).keySet());

        Optional<DateTime> minimumMessageTime = (ioConfig.getLateMessageRejectionPeriod().isPresent() ? Optional.of(
            DateTime.now().minus(ioConfig.getLateMessageRejectionPeriod().get())
        ) : Optional.<DateTime>absent());

        taskGroups.put(groupId, new TaskGroup(generateStartingOffsetsForPartitionGroup(groupId), minimumMessageTime));
      }
    }

    // iterate through all the current task groups and make sure each one has the desired number of replica tasks
    boolean createdTask = false;
    for (Map.Entry<Integer, TaskGroup> entry : taskGroups.entrySet()) {
      TaskGroup taskGroup = entry.getValue();
      Integer groupId = entry.getKey();

      if (ioConfig.getReplicas() > taskGroup.tasks.size()) {
        log.info(
            "Number of tasks [%d] does not match configured numReplicas [%d] in task group [%d], creating more tasks",
            taskGroup.tasks.size(), ioConfig.getReplicas(), groupId
        );
        createKafkaTasksForGroup(groupId, ioConfig.getReplicas() - taskGroup.tasks.size());
        createdTask = true;
      }
    }

    if (createdTask && firstRunTime.isBeforeNow()) {
      // Schedule a run event after a short delay to update our internal data structures with the new tasks that were
      // just created. This is mainly for the benefit of the status API in situations where the run period is lengthy.
      scheduledExec.schedule(buildRunTask(), 5000, TimeUnit.MILLISECONDS);
    }
  }

  private void createKafkaTasksForGroup(int groupId, int replicas)
  {
    Map<Integer, Long> startPartitions = taskGroups.get(groupId).partitionOffsets;
    Map<Integer, Long> endPartitions = new HashMap<>();
    for (Integer partition : startPartitions.keySet()) {
      endPartitions.put(partition, Long.MAX_VALUE);
    }

    String sequenceName = generateSequenceName(groupId);

    Map<String, String> consumerProperties = Maps.newHashMap(ioConfig.getConsumerProperties());
    DateTime minimumMessageTime = taskGroups.get(groupId).minimumMessageTime.orNull();

    KafkaIOConfig kafkaIOConfig = new KafkaIOConfig(
        sequenceName,
        new KafkaPartitions(ioConfig.getTopic(), startPartitions),
        new KafkaPartitions(ioConfig.getTopic(), endPartitions),
        consumerProperties,
        true,
        false,
        minimumMessageTime
    );

    for (int i = 0; i < replicas; i++) {
      String taskId = Joiner.on("_").join(sequenceName, getRandomId());
      KafkaIndexTask indexTask = new KafkaIndexTask(
          taskId,
          new TaskResource(sequenceName, 1),
          spec.getDataSchema(),
          spec.getTuningConfig(),
          kafkaIOConfig,
          ImmutableMap.<String, Object>of(),
          null
      );

      Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
      if (taskQueue.isPresent()) {
        try {
          taskQueue.get().add(indexTask);
        }
        catch (EntryExistsException e) {
          log.error("Tried to add task [%s] but it already exists", indexTask.getId());
        }
      } else {
        log.error("Failed to get task queue because I'm not the leader!");
      }
    }
  }

  private Map<Integer, Long> generateStartingOffsetsForPartitionGroup(int groupId)
  {
    Map<Integer, Long> startingOffsets = new HashMap<>();
    for (Map.Entry<Integer, Long> entry : partitionGroups.get(groupId).entrySet()) {
      Integer partition = entry.getKey();
      Long offset = entry.getValue();

      if (offset != null) {
        // if we are given a startingOffset (set by a previous task group which is pending completion) then use it
        startingOffsets.put(partition, offset);
      } else {
        // if we don't have a startingOffset (first run or we had some previous failures and reset the offsets) then
        // get the offset from metadata storage (if available) or Kafka (otherwise)
        startingOffsets.put(partition, getOffsetFromStorageForPartition(partition));
      }
    }
    return startingOffsets;
  }

  /**
   * Queries the dataSource metadata table to see if there is a previous ending offset for this partition. If it doesn't
   * find any data, it will retrieve the latest or earliest Kafka offset depending on the useEarliestOffset config.
   */
  private long getOffsetFromStorageForPartition(int partition)
  {
    long offset;
    Map<Integer, Long> metadataOffsets = getOffsetsFromMetadataStorage();
    if (metadataOffsets.get(partition) != null) {
      offset = metadataOffsets.get(partition);
      log.debug("Getting offset [%,d] from metadata storage for partition [%d]", offset, partition);

      long latestKafkaOffset = getOffsetFromKafkaForPartition(partition, false);
      if (offset > latestKafkaOffset) {
        throw new ISE(
            "Offset in metadata storage [%,d] > latest Kafka offset [%,d] for partition [%d]. If your Kafka offsets have"
            + " been reset, you will need to remove the entry for [%s] from the dataSource table.",
            offset,
            latestKafkaOffset,
            partition,
            dataSource
        );
      }

    } else {
      offset = getOffsetFromKafkaForPartition(partition, ioConfig.isUseEarliestOffset());
      log.debug("Getting offset [%,d] from Kafka for partition [%d]", offset, partition);
    }

    return offset;
  }

  private Map<Integer, Long> getOffsetsFromMetadataStorage()
  {
    DataSourceMetadata dataSourceMetadata = indexerMetadataStorageCoordinator.getDataSourceMetadata(dataSource);
    if (dataSourceMetadata != null && dataSourceMetadata instanceof KafkaDataSourceMetadata) {
      KafkaPartitions partitions = ((KafkaDataSourceMetadata) dataSourceMetadata).getKafkaPartitions();
      if (partitions != null) {
        if (!ioConfig.getTopic().equals(partitions.getTopic())) {
          log.warn(
              "Topic in metadata storage [%s] doesn't match spec topic [%s], ignoring stored offsets",
              partitions.getTopic(),
              ioConfig.getTopic()
          );
          return ImmutableMap.of();
        } else if (partitions.getPartitionOffsetMap() != null) {
          return partitions.getPartitionOffsetMap();
        }
      }
    }

    return ImmutableMap.of();
  }

  private long getOffsetFromKafkaForPartition(int partition, boolean useEarliestOffset)
  {
    TopicPartition topicPartition = new TopicPartition(ioConfig.getTopic(), partition);
    if (!consumer.assignment().contains(topicPartition)) {
      consumer.assign(Lists.newArrayList(topicPartition));
    }

    if (useEarliestOffset) {
      consumer.seekToBeginning(topicPartition);
    } else {
      consumer.seekToEnd(topicPartition);
    }

    return consumer.position(topicPartition);
  }

  /**
   * Compares the sequence name from the task with one generated for the task's group ID and returns false if they do
   * not match. The sequence name is generated from a hash of the dataSchema, tuningConfig, starting offsets, and the
   * minimumMessageTime if set.
   */
  private boolean isTaskCurrent(int taskGroupId, String taskId)
  {
    Optional<Task> taskOptional = taskStorage.getTask(taskId);
    if (!taskOptional.isPresent() || !(taskOptional.get() instanceof KafkaIndexTask)) {
      return false;
    }

    String taskSequenceName = ((KafkaIndexTask) taskOptional.get()).getIOConfig().getBaseSequenceName();

    return generateSequenceName(taskGroupId).equals(taskSequenceName);
  }

  private void stopTasksInGroup(TaskGroup taskGroup)
  {
    if (taskGroup == null) {
      return;
    }

    for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
      if (!entry.getValue().status.isComplete()) {
        stopTask(entry.getKey(), false);
      }
    }
  }

  private void stopTask(final String id, final boolean publish)
  {
    if (!taskInfoProvider.getTaskLocation(id).equals(TaskLocation.unknown())) {
      try {
        taskClient.stop(id, publish);
      }
      catch (Exception e) {
        log.warn(e, "Task [%s] failed to stop in a timely manner, killing task", id);
        killTask(id);
      }
    } else {
      killTask(id);
    }
  }

  private void killTask(final String id)
  {
    Optional<TaskQueue> taskQueue = taskMaster.getTaskQueue();
    if (taskQueue.isPresent()) {
      taskQueue.get().shutdown(id);
    } else {
      log.error("Failed to get task queue because I'm not the leader!");
    }
  }

  private DateTime getTaskStartTime(final String id)
  {
    if (!taskInfoProvider.getTaskLocation(id).equals(TaskLocation.unknown())) {
      DateTime startTime = taskClient.getStartTime(id, false);
      log.debug("Received start time of [%s] from task [%s]", startTime, id);
      return startTime;
    }

    return null;
  }

  private Optional<KafkaIndexTask.Status> getTaskStatus(final String id)
  {
    if (!taskInfoProvider.getTaskLocation(id).equals(TaskLocation.unknown())) {
      try {
        return Optional.of(taskClient.getStatus(id));
      }
      catch (Exception e) {
        log.warn(e, "Failed to get status for task [%s]", id);
      }
    }

    return Optional.absent();
  }

  private Map<Integer, Long> getCurrentOffsets(final String id, final boolean retry)
  {
    if (!taskInfoProvider.getTaskLocation(id).equals(TaskLocation.unknown())) {
      try {
        return taskClient.getCurrentOffsets(id, retry);
      }
      catch (Exception e) {
        // this happens regularly if generateReport() is frequently hit and a task is in transition and isn't fatal so
        // downgrade to info without stack trace
        log.info("Failed to get current offsets for task [%s]", id);
      }
    }

    return ImmutableMap.of();
  }

  private int getTaskGroupIdForPartition(int partition)
  {
    return partition % ioConfig.getTaskCount();
  }

  private boolean isTaskInPendingCompletionGroups(String taskId)
  {
    for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
      for (TaskGroup taskGroup : taskGroups) {
        if (taskGroup.tasks.containsKey(taskId)) {
          return true;
        }
      }
    }
    return false;
  }

  private KafkaSupervisorReport generateReport(boolean includeOffsets)
  {
    int numPartitions = 0;
    for (Map<Integer, Long> partitionGroup : partitionGroups.values()) {
      numPartitions += partitionGroup.size();
    }

    KafkaSupervisorReport report = new KafkaSupervisorReport(
        dataSource,
        DateTime.now(),
        ioConfig.getTopic(),
        numPartitions,
        ioConfig.getReplicas(),
        ioConfig.getTaskDuration().getMillis() / 1000
    );

    try {
      for (TaskGroup taskGroup : taskGroups.values()) {
        for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
          String taskId = entry.getKey();
          DateTime startTime = entry.getValue().startTime;
          Long remainingSeconds = null;
          if (startTime != null) {
            remainingSeconds = Math.max(
                0,
                ioConfig.getTaskDuration().getMillis() - (DateTime.now().getMillis() - startTime
                    .getMillis())
            ) / 1000;
          }

          report.addActiveTask(
              taskId,
              (includeOffsets ? taskGroup.partitionOffsets : null),
              (includeOffsets ? getCurrentOffsets(taskId, false) : null),
              startTime,
              remainingSeconds
          );
        }
      }

      for (List<TaskGroup> taskGroups : pendingCompletionTaskGroups.values()) {
        for (TaskGroup taskGroup : taskGroups) {
          for (Map.Entry<String, TaskData> entry : taskGroup.tasks.entrySet()) {
            String taskId = entry.getKey();
            DateTime startTime = entry.getValue().startTime;
            Long remainingSeconds = null;
            if (taskGroup.completionTimeout != null) {
              remainingSeconds = Math.max(0, taskGroup.completionTimeout.getMillis() - DateTime.now().getMillis())
                                 / 1000;
            }

            report.addPublishingTask(
                taskId,
                (includeOffsets ? taskGroup.partitionOffsets : null),
                (includeOffsets ? getCurrentOffsets(taskId, false) : null),
                startTime,
                remainingSeconds
            );
          }
        }
      }
    }
    catch (Exception e) {
      log.warn(e, "Failed to generate status report");
    }

    return report;
  }

  private Runnable buildRunTask()
  {
    return new Runnable()
    {
      @Override
      public void run()
      {
        notices.add(new RunNotice());
      }
    };
  }
}
