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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.indexer.TaskInfo;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SQLMetadataStorageActionHandlerTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  private static final ObjectMapper JSON_MAPPER = new DefaultObjectMapper();
  private SQLMetadataStorageActionHandler<Map<String, Integer>, Map<String, Integer>, Map<String, String>, Map<String, Integer>> handler;

  @Before
  public void setUp()
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();

    final String entryType = "entry";
    final String entryTable = "entries";
    final String logTable = "logs";
    final String lockTable = "locks";

    connector.createEntryTable(entryTable);
    connector.createLockTable(lockTable, entryType);
    connector.createLogTable(logTable, entryType);

    handler = new DerbyMetadataStorageActionHandler<>(
        connector,
        JSON_MAPPER,
        new MetadataStorageActionHandlerTypes<Map<String, Integer>, Map<String, Integer>, Map<String, String>, Map<String, Integer>>()
        {
          @Override
          public TypeReference<Map<String, Integer>> getEntryType()
          {
            return new TypeReference<Map<String, Integer>>()
            {
            };
          }

          @Override
          public TypeReference<Map<String, Integer>> getStatusType()
          {
            return new TypeReference<Map<String, Integer>>()
            {
            };
          }

          @Override
          public TypeReference<Map<String, String>> getLogType()
          {
            return JacksonUtils.TYPE_REFERENCE_MAP_STRING_STRING;
          }

          @Override
          public TypeReference<Map<String, Integer>> getLockType()
          {
            return new TypeReference<Map<String, Integer>>()
            {
            };
          }
        },
        entryType,
        entryTable,
        logTable,
        lockTable
    );
  }

  @Test
  public void testEntryAndStatus() throws Exception
  {
    Map<String, Integer> entry = ImmutableMap.of("numericId", 1234);
    Map<String, Integer> status1 = ImmutableMap.of("count", 42);
    Map<String, Integer> status2 = ImmutableMap.of("count", 42, "temp", 1);

    final String entryId = "1234";

    handler.insert(entryId, DateTimes.of("2014-01-02T00:00:00.123"), "testDataSource", entry, true, null);

    Assert.assertEquals(
        Optional.of(entry),
        handler.getEntry(entryId)
    );

    Assert.assertEquals(Optional.absent(), handler.getEntry("non_exist_entry"));

    Assert.assertEquals(Optional.absent(), handler.getStatus(entryId));

    Assert.assertEquals(Optional.absent(), handler.getStatus("non_exist_entry"));

    Assert.assertTrue(handler.setStatus(entryId, true, status1));

    Assert.assertEquals(
        ImmutableList.of(Pair.of(entry, status1)),
        handler.getActiveTaskInfo(null).stream()
               .map(taskInfo -> Pair.of(taskInfo.getTask(), taskInfo.getStatus()))
               .collect(Collectors.toList())
    );

    Assert.assertTrue(handler.setStatus(entryId, true, status2));

    Assert.assertEquals(
        ImmutableList.of(Pair.of(entry, status2)),
        handler.getActiveTaskInfo(null).stream()
               .map(taskInfo -> Pair.of(taskInfo.getTask(), taskInfo.getStatus()))
               .collect(Collectors.toList())
    );

    Assert.assertEquals(
        ImmutableList.of(),
        handler.getCompletedTaskInfo(DateTimes.of("2014-01-01"), null, null)
    );

    Assert.assertTrue(handler.setStatus(entryId, false, status1));

    Assert.assertEquals(
        Optional.of(status1),
        handler.getStatus(entryId)
    );

    // inactive statuses cannot be updated, this should fail
    Assert.assertFalse(handler.setStatus(entryId, false, status2));

    Assert.assertEquals(
        Optional.of(status1),
        handler.getStatus(entryId)
    );

    Assert.assertEquals(
        Optional.of(entry),
        handler.getEntry(entryId)
    );

    Assert.assertEquals(
        ImmutableList.of(),
        handler.getCompletedTaskInfo(DateTimes.of("2014-01-03"), null, null)
    );

    Assert.assertEquals(
        ImmutableList.of(status1),
        handler.getCompletedTaskInfo(DateTimes.of("2014-01-01"), null, null)
               .stream()
               .map(TaskInfo::getStatus)
               .collect(Collectors.toList())
    );
  }

  @Test
  public void testGetRecentStatuses() throws EntryExistsException
  {
    for (int i = 1; i < 11; i++) {
      final String entryId = "abcd_" + i;
      final Map<String, Integer> entry = ImmutableMap.of("a", i);
      final Map<String, Integer> status = ImmutableMap.of("count", i * 10);

      handler.insert(entryId, DateTimes.of(StringUtils.format("2014-01-%02d", i)), "test", entry, false, status);
    }

    final List<TaskInfo<Map<String, Integer>, Map<String, Integer>>> statuses = handler.getCompletedTaskInfo(
        DateTimes.of("2014-01-01"),
        7,
        null
    );
    Assert.assertEquals(7, statuses.size());
    int i = 10;
    for (TaskInfo<Map<String, Integer>, Map<String, Integer>> status : statuses) {
      Assert.assertEquals(ImmutableMap.of("count", i-- * 10), status.getStatus());
    }
  }

  @Test
  public void testGetRecentStatuses2() throws EntryExistsException
  {
    for (int i = 1; i < 6; i++) {
      final String entryId = "abcd_" + i;
      final Map<String, Integer> entry = ImmutableMap.of("a", i);
      final Map<String, Integer> status = ImmutableMap.of("count", i * 10);

      handler.insert(entryId, DateTimes.of(StringUtils.format("2014-01-%02d", i)), "test", entry, false, status);
    }

    final List<TaskInfo<Map<String, Integer>, Map<String, Integer>>> statuses = handler.getCompletedTaskInfo(
        DateTimes.of("2014-01-01"),
        10,
        null
    );
    Assert.assertEquals(5, statuses.size());
    int i = 5;
    for (TaskInfo<Map<String, Integer>, Map<String, Integer>> status : statuses) {
      Assert.assertEquals(ImmutableMap.of("count", i-- * 10), status.getStatus());
    }
  }

  @Test(timeout = 60_000L)
  public void testRepeatInsert() throws Exception
  {
    final String entryId = "abcd";
    Map<String, Integer> entry = ImmutableMap.of("a", 1);
    Map<String, Integer> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status);

    thrown.expect(EntryExistsException.class);
    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status);
  }

  @Test
  public void testLogs() throws Exception
  {
    final String entryId = "abcd";
    Map<String, Integer> entry = ImmutableMap.of("a", 1);
    Map<String, Integer> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status);

    Assert.assertEquals(
        ImmutableList.of(),
        handler.getLogs("non_exist_entry")
    );

    Assert.assertEquals(
        ImmutableMap.of(),
        handler.getLocks(entryId)
    );

    final ImmutableMap<String, String> log1 = ImmutableMap.of("logentry", "created");
    final ImmutableMap<String, String> log2 = ImmutableMap.of("logentry", "updated");

    Assert.assertTrue(handler.addLog(entryId, log1));
    Assert.assertTrue(handler.addLog(entryId, log2));

    Assert.assertEquals(
        ImmutableList.of(log1, log2),
        handler.getLogs(entryId)
    );
  }


  @Test
  public void testLocks() throws Exception
  {
    final String entryId = "ABC123";
    Map<String, Integer> entry = ImmutableMap.of("a", 1);
    Map<String, Integer> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status);

    Assert.assertEquals(
        ImmutableMap.<Long, Map<String, Integer>>of(),
        handler.getLocks("non_exist_entry")
    );

    Assert.assertEquals(
        ImmutableMap.<Long, Map<String, Integer>>of(),
        handler.getLocks(entryId)
    );

    final ImmutableMap<String, Integer> lock1 = ImmutableMap.of("lock", 1);
    final ImmutableMap<String, Integer> lock2 = ImmutableMap.of("lock", 2);

    Assert.assertTrue(handler.addLock(entryId, lock1));
    Assert.assertTrue(handler.addLock(entryId, lock2));

    final Map<Long, Map<String, Integer>> locks = handler.getLocks(entryId);
    Assert.assertEquals(2, locks.size());

    Assert.assertEquals(
        ImmutableSet.<Map<String, Integer>>of(lock1, lock2),
        new HashSet<>(locks.values())
    );

    long lockId = locks.keySet().iterator().next();
    handler.removeLock(lockId);
    locks.remove(lockId);

    final Map<Long, Map<String, Integer>> updated = handler.getLocks(entryId);
    Assert.assertEquals(
        new HashSet<>(locks.values()),
        new HashSet<>(updated.values())
    );
    Assert.assertEquals(updated.keySet(), locks.keySet());
  }

  @Test
  public void testReplaceLock() throws EntryExistsException
  {
    final String entryId = "ABC123";
    Map<String, Integer> entry = ImmutableMap.of("a", 1);
    Map<String, Integer> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status);

    Assert.assertEquals(
        ImmutableMap.<Long, Map<String, Integer>>of(),
        handler.getLocks("non_exist_entry")
    );

    Assert.assertEquals(
        ImmutableMap.<Long, Map<String, Integer>>of(),
        handler.getLocks(entryId)
    );

    final ImmutableMap<String, Integer> lock1 = ImmutableMap.of("lock", 1);
    final ImmutableMap<String, Integer> lock2 = ImmutableMap.of("lock", 2);

    Assert.assertTrue(handler.addLock(entryId, lock1));

    final Long lockId1 = handler.getLockId(entryId, lock1);
    Assert.assertNotNull(lockId1);

    Assert.assertTrue(handler.replaceLock(entryId, lockId1, lock2));
  }

  @Test
  public void testGetLockId() throws EntryExistsException
  {
    final String entryId = "ABC123";
    Map<String, Integer> entry = ImmutableMap.of("a", 1);
    Map<String, Integer> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, DateTimes.of("2014-01-01"), "test", entry, true, status);

    Assert.assertEquals(
        ImmutableMap.<Long, Map<String, Integer>>of(),
        handler.getLocks("non_exist_entry")
    );

    Assert.assertEquals(
        ImmutableMap.<Long, Map<String, Integer>>of(),
        handler.getLocks(entryId)
    );

    final ImmutableMap<String, Integer> lock1 = ImmutableMap.of("lock", 1);
    final ImmutableMap<String, Integer> lock2 = ImmutableMap.of("lock", 2);

    Assert.assertTrue(handler.addLock(entryId, lock1));

    Assert.assertNotNull(handler.getLockId(entryId, lock1));
    Assert.assertNull(handler.getLockId(entryId, lock2));
  }

  @Test
  public void testRemoveTasksOlderThan() throws Exception
  {
    final String entryId1 = "1234";
    Map<String, Integer> entry1 = ImmutableMap.of("numericId", 1234);
    Map<String, Integer> status1 = ImmutableMap.of("count", 42, "temp", 1);
    handler.insert(entryId1, DateTimes.of("2014-01-01T00:00:00.123"), "testDataSource", entry1, false, status1);
    Assert.assertTrue(handler.addLog(entryId1, ImmutableMap.of("logentry", "created")));

    final String entryId2 = "ABC123";
    Map<String, Integer> entry2 = ImmutableMap.of("a", 1);
    Map<String, Integer> status2 = ImmutableMap.of("count", 42);
    handler.insert(entryId2, DateTimes.of("2014-01-01T00:00:00.123"), "test", entry2, true, status2);
    Assert.assertTrue(handler.addLog(entryId2, ImmutableMap.of("logentry", "created")));

    final String entryId3 = "DEF5678";
    Map<String, Integer> entry3 = ImmutableMap.of("numericId", 5678);
    Map<String, Integer> status3 = ImmutableMap.of("count", 21, "temp", 2);
    handler.insert(entryId3, DateTimes.of("2014-01-02T12:00:00.123"), "testDataSource", entry3, false, status3);
    Assert.assertTrue(handler.addLog(entryId3, ImmutableMap.of("logentry", "created")));

    Assert.assertEquals(Optional.of(entry1), handler.getEntry(entryId1));
    Assert.assertEquals(Optional.of(entry2), handler.getEntry(entryId2));
    Assert.assertEquals(Optional.of(entry3), handler.getEntry(entryId3));

    Assert.assertEquals(
        ImmutableList.of(entryId2),
        handler.getActiveTaskInfo(null).stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())
    );

    Assert.assertEquals(
        ImmutableList.of(entryId3, entryId1),
        handler.getCompletedTaskInfo(DateTimes.of("2014-01-01"), null, null).stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())

    );

    handler.removeTasksOlderThan(DateTimes.of("2014-01-02").getMillis());
    // active task not removed.
    Assert.assertEquals(
        ImmutableList.of(entryId2),
        handler.getActiveTaskInfo(null).stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())
    );
    Assert.assertEquals(
        ImmutableList.of(entryId3),
        handler.getCompletedTaskInfo(DateTimes.of("2014-01-01"), null, null).stream()
               .map(taskInfo -> taskInfo.getId())
               .collect(Collectors.toList())

    );
    // tasklogs
    Assert.assertEquals(0, handler.getLogs(entryId1).size());
    Assert.assertEquals(1, handler.getLogs(entryId2).size());
    Assert.assertEquals(1, handler.getLogs(entryId3).size());
  }
}
