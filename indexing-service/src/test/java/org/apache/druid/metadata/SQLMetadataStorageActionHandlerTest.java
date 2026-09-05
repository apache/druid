/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.metadata;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexer.TaskIdStatus;
import org.apache.druid.indexer.TaskIdentifier;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.TaskLock;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.TimeChunkLock;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.TaskLookup.ActiveTaskLookup;
import org.apache.druid.metadata.TaskLookup.CompleteTaskLookup;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.InDimFilter;
import org.apache.druid.query.filter.OrDimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.query.filter.TypedInFilter;
import org.apache.druid.segment.column.ColumnType;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Timeout.ThreadMode;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class SQLMetadataStorageActionHandlerTest
{
  @RegisterExtension
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();

  private static final Random RANDOM = new Random(1);

  private SQLMetadataStorageActionHandler handler;

  private final String entryTable = "entries";

  @BeforeEach
  public void setUp()
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();

    final String lockTable = "locks";

    connector.prepareTaskEntryTable(entryTable);
    connector.createLockTable(lockTable);

    handler = new DerbyMetadataStorageActionHandler(
        connector,
        JSON_MAPPER,
        entryTable,
        lockTable
    );
  }

  @Test
  public void testEntryAndStatus()
  {
    Task entry = NoopTask.create();
    TaskStatus status1 = TaskStatus.running(entry.getId());
    TaskStatus status2 = TaskStatus.success(entry.getId());

    final String entryId = entry.getId();

    handler.insert(entryId, DateTimes.of("2014-01-02T00:00:00.123"), "testDataSource", entry, true, null, "type", "group");

    Assertions.assertEquals(Optional.of(entry), handler.getEntry(entryId));
    Assertions.assertEquals(Optional.absent(), handler.getEntry("non_exist_entry"));
    Assertions.assertEquals(Optional.absent(), handler.getStatus(entryId));
    Assertions.assertEquals(Optional.absent(), handler.getStatus("non_exist_entry"));
    Assertions.assertTrue(handler.setStatus(entryId, true, status1));

    Assertions.assertEquals(
        ImmutableList.of(Pair.of(entry, status1)),
        handler.getTaskInfos(ActiveTaskLookup.getInstance(), null).stream()
               .map(taskInfo -> Pair.of(taskInfo.getTask(), taskInfo.getStatus()))
               .collect(Collectors.toList())
    );

    Assertions.assertTrue(handler.setStatus(entryId, true, status2));

    Assertions.assertEquals(
        ImmutableList.of(Pair.of(entry, status2)),
        handler.getTaskInfos(ActiveTaskLookup.getInstance(), null).stream()
               .map(taskInfo -> Pair.of(taskInfo.getTask(), taskInfo.getStatus()))
               .collect(Collectors.toList())
    );

    Assertions.assertEquals(
        ImmutableList.of(),
        handler.getTaskInfos(CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2014-01-01")), null)
    );

    Assertions.assertTrue(handler.setStatus(entryId, false, status1));

    Assertions.assertEquals(
        Optional.of(status1),
        handler.getStatus(entryId)
    );

    // inactive statuses cannot be updated, this should fail
    Assertions.assertFalse(handler.setStatus(entryId, false, status2));

    Assertions.assertEquals(Optional.of(status1), handler.getStatus(entryId));
    Assertions.assertEquals(Optional.of(entry), handler.getEntry(entryId));
    Assertions.assertEquals(
        ImmutableList.of(),
        handler.getTaskInfos(CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2014-01-03")), null)
    );
    Assertions.assertEquals(
        ImmutableList.of(status1),
        handler.getTaskInfos(CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2014-01-01")), null)
               .stream()
               .map(TaskInfo::getStatus)
               .collect(Collectors.toList())
    );
  }

  @Test
  public void testGetRecentStatuses()
  {
    for (int i = 1; i < 11; i++) {
      final Task entry = NoopTask.create();
      final String entryId = entry.getId();
      final TaskStatus status = TaskStatus.running(entry.getId());

      handler.insert(entryId, DateTimes.of(StringUtils.format("2014-01-%02d", i)), "test", entry, false, status, "type", "group");
    }

    final List<TaskInfo> statuses = handler.getTaskInfos(
        CompleteTaskLookup.withTasksCreatedPriorTo(7, DateTimes.of("2014-01-01")),
        null
    );
    Assertions.assertEquals(7, statuses.size());
    for (TaskInfo status : statuses) {
      Assertions.assertEquals(TaskState.RUNNING, status.getStatus().getStatusCode());
    }
  }

  @Test
  public void testGetRecentStatuses2()
  {
    for (int i = 1; i < 6; i++) {
      final Task entry = NoopTask.create();
      final String entryId = entry.getId();
      final TaskStatus status = TaskStatus.running(entry.getId());

      handler.insert(entryId, DateTimes.of(StringUtils.format("2014-01-%02d", i)), "test", entry, false, status, "type", "group");
    }

    final List<TaskInfo> statuses = handler.getTaskInfos(
        CompleteTaskLookup.withTasksCreatedPriorTo(10, DateTimes.of("2014-01-01")),
        null
    );
    Assertions.assertEquals(5, statuses.size());
    for (TaskInfo status : statuses) {
      Assertions.assertEquals(TaskState.RUNNING, status.getStatus().getStatusCode());
    }
  }

  @Test
  @Timeout(value = 60_000L, unit = TimeUnit.MILLISECONDS, threadMode = ThreadMode.SEPARATE_THREAD)
  public void testDuplicateInsertThrowsEntryExistsException()
  {
    Task entry = NoopTask.create();
    final String entryId = entry.getId();
    TaskStatus status = TaskStatus.running(entryId);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status, "type", "group");

    DruidException exception = Assertions.assertThrows(
        DruidException.class,
        () -> handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status, "type", "group")
    );
    Assertions.assertEquals("invalidInput", exception.getErrorCode());
    Assertions.assertEquals(StringUtils.format("Task [%s] already exists", entryId), exception.getMessage());
  }

  @Test
  public void testLocks()
  {
    Task entry = NoopTask.create();
    final String entryId = entry.getId();
    TaskStatus status = TaskStatus.running(entryId);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status, "type", "group");

    Assertions.assertTrue(handler.getLocks("non_exist_entry").isEmpty());

    Assertions.assertTrue(handler.getLocks(entryId).isEmpty());

    final TaskLock lock1 = createRandomLock(entry);
    final TaskLock lock2 = createRandomLock(entry);

    Assertions.assertTrue(handler.addLock(entryId, lock1));
    Assertions.assertTrue(handler.addLock(entryId, lock2));

    final Map<Long, TaskLock> locks = handler.getLocks(entryId);
    Assertions.assertEquals(2, locks.size());

    Assertions.assertEquals(
        Set.of(lock1, lock2),
        new HashSet<>(locks.values())
    );

    long lockId = locks.keySet().iterator().next();
    handler.removeLock(lockId);
    locks.remove(lockId);

    final Map<Long, TaskLock> updated = handler.getLocks(entryId);
    Assertions.assertEquals(
        new HashSet<>(locks.values()),
        new HashSet<>(updated.values())
    );
    Assertions.assertEquals(updated.keySet(), locks.keySet());
  }

  @Test
  public void testReplaceLock()
  {
    Task entry = NoopTask.create();
    final String entryId = entry.getId();
    TaskStatus status = TaskStatus.running(entryId);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status, "type", "group");

    Assertions.assertEquals(
        ImmutableMap.<Long, Map<String, Object>>of(),
        handler.getLocks("non_exist_entry")
    );

    Assertions.assertEquals(
        ImmutableMap.<Long, Map<String, Object>>of(),
        handler.getLocks(entryId)
    );

    final TaskLock lock1 = createRandomLock(entry);
    final TaskLock lock2 = createRandomLock(entry);

    Assertions.assertTrue(handler.addLock(entryId, lock1));

    final Long lockId1 = handler.getLockId(entryId, lock1);
    Assertions.assertNotNull(lockId1);

    Assertions.assertTrue(handler.replaceLock(entryId, lockId1, lock2));
  }

  @Test
  public void testGetLockId()
  {
    Task entry = NoopTask.create();
    final String entryId = entry.getId();
    TaskStatus status = TaskStatus.running(entryId);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status, "type", "group");

    Assertions.assertEquals(
        ImmutableMap.<Long, Map<String, Object>>of(),
        handler.getLocks("non_exist_entry")
    );

    Assertions.assertEquals(
        ImmutableMap.<Long, Map<String, Object>>of(),
        handler.getLocks(entryId)
    );

    final TaskLock lock1 = createRandomLock(entry);
    final TaskLock lock2 = createRandomLock(entry);

    Assertions.assertTrue(handler.addLock(entryId, lock1));

    Assertions.assertNotNull(handler.getLockId(entryId, lock1));
    Assertions.assertNull(handler.getLockId(entryId, lock2));
  }

  @Test
  public void testRemoveTasksOlderThan()
  {
    Task entry1 = NoopTask.create();
    final String entryId1 = entry1.getId();
    TaskStatus status1 = TaskStatus.running(entryId1);
    handler.insert(entryId1, DateTimes.of("2014-01-01T00:00:00.123"), "testDataSource", entry1, false, status1, "type", "group");

    Task entry2 = NoopTask.create();
    final String entryId2 = entry2.getId();
    TaskStatus status2 = TaskStatus.running(entryId2);
    handler.insert(entryId2, DateTimes.of("2014-01-01T00:00:00.123"), "test", entry2, true, status2, "type", "group");

    Task entry3 = NoopTask.create();
    final String entryId3 = entry3.getId();
    TaskStatus status3 = TaskStatus.running(entryId2);
    handler.insert(entryId3, DateTimes.of("2014-01-02T12:00:00.123"), "testDataSource", entry3, false, status3, "type", "group");

    Assertions.assertEquals(Optional.of(entry1), handler.getEntry(entryId1));
    Assertions.assertEquals(Optional.of(entry2), handler.getEntry(entryId2));
    Assertions.assertEquals(Optional.of(entry3), handler.getEntry(entryId3));

    Assertions.assertEquals(
        ImmutableList.of(entryId2),
        handler.getTaskInfos(ActiveTaskLookup.getInstance(), null).stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())
    );

    Assertions.assertEquals(
        ImmutableList.of(entryId3, entryId1),
        handler.getTaskInfos(CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2014-01-01")), null)
               .stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())

    );

    handler.removeTasksOlderThan(DateTimes.of("2014-01-02").getMillis());
    // active task not removed.
    Assertions.assertEquals(
        ImmutableList.of(entryId2),
        handler.getTaskInfos(ActiveTaskLookup.getInstance(), null).stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())
    );
    Assertions.assertEquals(
        ImmutableList.of(entryId3),
        handler.getTaskInfos(CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2014-01-01")), null)
               .stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())

    );
  }

  @Test
  public void testMigration()
  {
    int numActiveTasks = 123;
    for (int i = 0; i < numActiveTasks; i++) {
      insertTaskInfo(createRandomTaskInfo(TaskState.RUNNING), false);
    }

    int numCompletedTasks = 101;
    for (int i = 0; i < numCompletedTasks; i++) {
      insertTaskInfo(createRandomTaskInfo(TaskState.SUCCESS), false);
    }

    Assertions.assertEquals(numActiveTasks + numCompletedTasks, getUnmigratedTaskCount().intValue());

    handler.populateTaskTypeAndGroupId();

    Assertions.assertEquals(0, getUnmigratedTaskCount().intValue());
  }

  @Test
  public void testGetTaskStatusPlusListInternal()
  {
    // SETUP
    TaskInfo activeUnaltered = createRandomTaskInfo(TaskState.RUNNING);
    insertTaskInfo(activeUnaltered, false);

    TaskInfo completedUnaltered = createRandomTaskInfo(TaskState.SUCCESS);
    insertTaskInfo(completedUnaltered, false);

    TaskInfo activeAltered = createRandomTaskInfo(TaskState.RUNNING);
    insertTaskInfo(activeAltered, true);

    TaskInfo completedAltered = createRandomTaskInfo(TaskState.SUCCESS);
    insertTaskInfo(completedAltered, true);

    Map<TaskLookup.TaskLookupType, TaskLookup> taskLookups = new HashMap<>();
    taskLookups.put(TaskLookup.TaskLookupType.ACTIVE, ActiveTaskLookup.getInstance());
    taskLookups.put(TaskLookup.TaskLookupType.COMPLETE, CompleteTaskLookup.of(null, Duration.millis(86400000)));

    List<TaskIdStatus> taskMetadataInfos;

    // BEFORE MIGRATION

    // Payload based fetch. task type and groupid will be populated
    taskMetadataInfos = handler.getTaskStatusList(taskLookups, null, true);
    Assertions.assertEquals(4, taskMetadataInfos.size());
    verifyTaskInfoToMetadataInfo(completedUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(completedAltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeAltered, taskMetadataInfos, false);

    // New columns based fetch before migration is complete. type and payload are null when altered = false
    taskMetadataInfos = handler.getTaskStatusList(taskLookups, null, false);
    Assertions.assertEquals(4, taskMetadataInfos.size());
    verifyTaskInfoToMetadataInfo(completedUnaltered, taskMetadataInfos, true);
    verifyTaskInfoToMetadataInfo(completedAltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeUnaltered, taskMetadataInfos, true);
    verifyTaskInfoToMetadataInfo(activeAltered, taskMetadataInfos, false);

    // MIGRATION
    handler.populateTaskTypeAndGroupId();

    // Payload based fetch. task type and groupid will still be populated in tasks tab
    taskMetadataInfos = handler.getTaskStatusList(taskLookups, null, true);
    Assertions.assertEquals(4, taskMetadataInfos.size());
    verifyTaskInfoToMetadataInfo(completedUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(completedAltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeAltered, taskMetadataInfos, false);

    // New columns based fetch after migration is complete. All data must be populated in the tasks table
    taskMetadataInfos = handler.getTaskStatusList(taskLookups, null, false);
    Assertions.assertEquals(4, taskMetadataInfos.size());
    verifyTaskInfoToMetadataInfo(completedUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(completedAltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeUnaltered, taskMetadataInfos, false);
    verifyTaskInfoToMetadataInfo(activeAltered, taskMetadataInfos, false);
  }

  @Test
  public void testGetTaskStatusListWithStorageFilters()
  {
    insertStorageFilterTestTasks();

    assertFilteredTaskIds(
        Set.of("task-1", "task-3"),
        List.of(new TypedInFilter("id", ColumnType.STRING, List.of("task-1", "task-3"), null, null))
    );
    // Null group_id and type values are retained as a migration-safe superset. The native filter remains residual.
    assertFilteredTaskIds(
        Set.of("task-1", "task-3", "task-legacy"),
        List.of(new SelectorDimFilter("group_id", "group-a", null))
    );
    assertFilteredTaskIds(
        Set.of("task-1", "task-legacy"),
        List.of(new SelectorDimFilter("type", "noop", null))
    );
    assertFilteredTaskIds(
        Set.of("task-2"),
        List.of(new SelectorDimFilter("datasource", "datasource-b", null))
    );
    assertFilteredTaskIds(
        Set.of("task-3"),
        List.of(new SelectorDimFilter("created_date", "2014-01-30T00:00:00.000Z", null))
    );
    assertFilteredTaskIds(
        Set.of("task-1", "task-legacy"),
        List.of(new SelectorDimFilter("status", TaskState.RUNNING.name(), null))
    );
    // SUCCESS and FAILED share the completed-task lookup; the native residual filter distinguishes them.
    assertFilteredTaskIds(
        Set.of("task-2", "task-3"),
        List.of(new SelectorDimFilter("status", TaskState.SUCCESS.name(), null))
    );
  }

  /**
   * Verifies SQL generation and binding for string-value filters:
   *
   * - `id = 'task-2'`
   * - `id IN ('task-1', 'task-3')`
   * - `id = 'task-1' OR id = 'task-3'`
   */
  @Test
  public void testGetTaskStatusListWithStringValueFilterVariants()
  {
    insertStorageFilterTestTasks();

    assertFilteredTaskIds(
        Set.of("task-2"),
        List.of(new EqualityFilter("id", ColumnType.STRING, "task-2", null))
    );
    assertFilteredTaskIds(
        Set.of("task-1", "task-3"),
        List.of(new InDimFilter("id", List.of("task-1", "task-3"), null))
    );
    assertFilteredTaskIds(
        Set.of("task-1", "task-3"),
        List.of(
            new OrDimFilter(
                new SelectorDimFilter("id", "task-1", null),
                new SelectorDimFilter("id", "task-3", null)
            )
        )
    );
  }

  /**
   * Verifies multiple predicates and the legacy datasource argument in the same metadata query:
   *
   * - `datasource = 'datasource-a' AND id IN ('task-1', 'task-3')`
   * - legacy datasource filter `datasource = 'datasource-a'`
   */
  @Test
  public void testGetTaskStatusListWithCombinedFiltersAndLegacyDatasource()
  {
    insertStorageFilterTestTasks();

    assertFilteredTaskIds(
        Set.of("task-1", "task-3", "task-legacy"),
        List.of(
            new SelectorDimFilter("datasource", "datasource-a", null),
            new InDimFilter("id", List.of("task-1", "task-3", "task-legacy"), null)
        )
    );
    assertFilteredTaskIds(Set.of("task-1", "task-3", "task-legacy"), "datasource-a");
  }

  private void assertFilteredTaskIds(
      final Set<String> expectedTaskIds,
      final List<DimFilter> pushdownFilters
  )
  {
    final Set<String> actualTaskIds = handler
        .getTaskStatusListWithFilter(taskLookups(), new TaskStorageQueryFilter(pushdownFilters), false)
        .stream()
        .map(taskStatus -> taskStatus.getTaskIdentifier().getId())
        .collect(Collectors.toSet());
    Assertions.assertEquals(expectedTaskIds, actualTaskIds);
  }

  private void assertFilteredTaskIds(final Set<String> expectedTaskIds, final String dataSource)
  {
    final Set<String> actualTaskIds = handler.getTaskStatusList(taskLookups(), dataSource, false)
                                                 .stream()
                                                 .map(taskStatus -> taskStatus.getTaskIdentifier().getId())
                                                 .collect(Collectors.toSet());
    Assertions.assertEquals(expectedTaskIds, actualTaskIds);
  }

  private static Map<TaskLookup.TaskLookupType, TaskLookup> taskLookups()
  {
    final Map<TaskLookup.TaskLookupType, TaskLookup> taskLookups = new HashMap<>();
    taskLookups.put(TaskLookup.TaskLookupType.ACTIVE, ActiveTaskLookup.getInstance());
    taskLookups.put(
        TaskLookup.TaskLookupType.COMPLETE,
        CompleteTaskLookup.withTasksCreatedPriorTo(null, DateTimes.of("2014-01-01"))
    );
    return taskLookups;
  }

  private void insertStorageFilterTestTasks()
  {
    insertTask("task-1", "group-a", "noop", "datasource-a", "2014-01-10", TaskState.RUNNING, true);
    insertTask("task-2", "group-b", "other", "datasource-b", "2014-01-20", TaskState.SUCCESS, true);
    insertTask("task-3", "group-a", "other", "datasource-a", "2014-01-30", TaskState.FAILED, true);
    insertTask("task-legacy", "group-a", "noop", "datasource-a", "2014-01-15", TaskState.RUNNING, false);
  }

  private void insertTask(
      final String id,
      final String groupId,
      final String storedType,
      final String dataSource,
      final String createdTime,
      final TaskState taskState,
      final boolean migrated
  )
  {
    final Task task = new NoopTask(id, groupId, dataSource, 1L, 0L, null);
    final TaskStatus status = new TaskStatus(id, taskState, 0L, null, TaskLocation.unknown());
    handler.insert(
        id,
        DateTimes.of(createdTime),
        dataSource,
        task,
        TaskState.RUNNING.equals(taskState),
        status,
        migrated ? storedType : null,
        migrated ? groupId : null
    );
  }

  private Integer getUnmigratedTaskCount()
  {
    return handler.getConnector().retryWithHandle(
        handle -> {
          String sql = StringUtils.format(
              "SELECT COUNT(*) FROM %s WHERE type is NULL or group_id is NULL",
              entryTable
          );
          try (final Statement statement = handle.getConnection().createStatement();
               final ResultSet resultSet = statement.executeQuery(sql)) {
            resultSet.next();
            return resultSet.getInt(1);
          }
        }
    );
  }

  private TaskLock createRandomLock(Task task)
  {
    final long intervalStart = RANDOM.nextLong();
    return new TimeChunkLock(
        TaskLockType.APPEND,
        task.getGroupId(),
        task.getDataSource(),
        Intervals.utc(intervalStart, intervalStart + 100),
        "v1",
        1
    );
  }

  private TaskInfo createRandomTaskInfo(TaskState taskState)
  {
    String id = UUID.randomUUID().toString();
    DateTime createdTime = DateTime.now(DateTimeZone.UTC);
    String datasource = UUID.randomUUID().toString();
    String groupId = UUID.randomUUID().toString();

    Task payload = new NoopTask(id, groupId, datasource, 1L, 0L, null);

    TaskStatus status = new TaskStatus(
        id,
        taskState,
        RANDOM.nextLong(),
        UUID.randomUUID().toString(),
        TaskLocation.create(UUID.randomUUID().toString(), 8080, 995)
    );

    return new TaskInfo(
        createdTime,
        status,
        payload
    );
  }

  private void insertTaskInfo(TaskInfo taskInfo, boolean altered)
  {
    try {
      handler.insert(
          taskInfo.getId(),
          taskInfo.getCreatedTime(),
          taskInfo.getDataSource(),
          taskInfo.getTask(),
          TaskState.RUNNING.equals(taskInfo.getStatus().getStatusCode()),
          taskInfo.getStatus(),
          altered ? taskInfo.getTask().getType() : null,
          altered ? taskInfo.getTask().getGroupId() : null
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void verifyTaskInfoToMetadataInfo(TaskInfo taskInfo,
                                            List<TaskIdStatus> taskMetadataInfos,
                                            boolean nullNewColumns)
  {
    for (TaskIdStatus taskMetadataInfo : taskMetadataInfos) {
      if (taskMetadataInfo.getTaskIdentifier().getId().equals(taskInfo.getId())) {
        verifyTaskInfoToMetadataInfo(taskInfo, taskMetadataInfo, nullNewColumns);
      }
      return;
    }
    Assertions.fail();
  }

  private void verifyTaskInfoToMetadataInfo(TaskInfo taskInfo,
                                            TaskIdStatus taskMetadataInfo,
                                            boolean nullNewColumns)
  {
    Assertions.assertEquals(taskInfo.getId(), taskMetadataInfo.getTaskIdentifier().getId());
    Assertions.assertEquals(taskInfo.getCreatedTime(), taskMetadataInfo.getCreatedTime());
    Assertions.assertEquals(taskInfo.getDataSource(), taskMetadataInfo.getDataSource());

    verifyTaskStatus(taskInfo.getStatus(), taskMetadataInfo.getStatus());

    Task task = taskInfo.getTask();
    TaskIdentifier taskIdentifier = taskMetadataInfo.getTaskIdentifier();
    Assertions.assertEquals(task.getId(), taskIdentifier.getId());
    if (nullNewColumns) {
      Assertions.assertNull(taskIdentifier.getGroupId());
      Assertions.assertNull(taskIdentifier.getType());
    } else {
      Assertions.assertEquals(task.getGroupId(), taskIdentifier.getGroupId());
      Assertions.assertEquals(task.getType(), taskIdentifier.getType());
    }
  }

  private void verifyTaskStatus(TaskStatus expected, TaskStatus actual)
  {
    Assertions.assertEquals(expected, actual);
  }
}
