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

package io.druid.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Pair;

import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;
import java.util.Map;

public class SQLMetadataStorageActionHandlerTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  @Rule
  public final ExpectedException thrown = ExpectedException.none();

  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private SQLMetadataStorageActionHandler<Map<String, Integer>, Map<String, Integer>, Map<String, String>, Map<String, Integer>> handler;

  @Before
  public void setUp() throws Exception
  {
    TestDerbyConnector connector = derbyConnectorRule.getConnector();

    final String entryType = "entry";
    final String entryTable = "entries";
    final String logTable = "logs";
    final String lockTable = "locks";


    connector.createEntryTable(entryTable);
    connector.createLockTable(lockTable, entryType);
    connector.createLogTable(logTable, entryType);


    handler = new SQLMetadataStorageActionHandler<>(
        connector,
        jsonMapper,
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
            return new TypeReference<Map<String, String>>()
            {
            };
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

    handler.insert(entryId, new DateTime("2014-01-02T00:00:00.123"), "testDataSource", entry, true, null);

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
        handler.getActiveEntriesWithStatus()
    );

    Assert.assertTrue(handler.setStatus(entryId, true, status2));

    Assert.assertEquals(
        ImmutableList.of(Pair.of(entry, status2)),
        handler.getActiveEntriesWithStatus()
    );

    Assert.assertEquals(
        ImmutableList.of(),
        handler.getInactiveStatusesSince(new DateTime("2014-01-01"))
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
        handler.getInactiveStatusesSince(new DateTime("2014-01-03"))
    );

    Assert.assertEquals(
        ImmutableList.of(status1),
        handler.getInactiveStatusesSince(new DateTime("2014-01-01"))
    );
  }

  @Test(timeout = 10_000L)
  public void testRepeatInsert() throws Exception
  {
    final String entryId = "abcd";
    Map<String, Integer> entry = ImmutableMap.of("a", 1);
    Map<String, Integer> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, new DateTime("2014-01-01"), "test", entry, true, status);

    thrown.expect(EntryExistsException.class);
    handler.insert(entryId, new DateTime("2014-01-01"), "test", entry, true, status);
  }

  @Test
  public void testLogs() throws Exception
  {
    final String entryId = "abcd";
    Map<String, Integer> entry = ImmutableMap.of("a", 1);
    Map<String, Integer> status = ImmutableMap.of("count", 42);

    handler.insert(entryId, new DateTime("2014-01-01"), "test", entry, true, status);

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

    handler.insert(entryId, new DateTime("2014-01-01"), "test", entry, true, status);

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
}
