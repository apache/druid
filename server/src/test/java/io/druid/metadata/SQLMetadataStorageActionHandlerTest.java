/*
 * Druid - a distributed column store.
 * Copyright (C) 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.metadata;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.Pair;
import io.druid.indexing.overlord.MetadataStorageActionHandlerTypes;
import io.druid.jackson.DefaultObjectMapper;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class SQLMetadataStorageActionHandlerTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();
  private TestDerbyConnector connector;
  private MetadataStorageTablesConfig tablesConfig = MetadataStorageTablesConfig.fromBase("test");

  @Before
  public void setUp() throws Exception {
    MetadataStorageConnectorConfig config = jsonMapper.readValue(
        "{"
        + "\"type\" : \"db\",\n"
        + "\"segmentTable\" : \"segments\"\n"
        + "}",
        MetadataStorageConnectorConfig.class
    );

    connector = new TestDerbyConnector(
        Suppliers.ofInstance(config),
        Suppliers.ofInstance(tablesConfig)
    );
  }

  @Test
  public void testTaskAndTaskStatus() throws Exception
  {
    SQLMetadataStorageActionHandler<Map<String, Integer>,Map<String, Integer>,Map<String, Integer>,Map<String, Integer>> handler = new SQLMetadataStorageActionHandler(connector, tablesConfig, jsonMapper, new MetadataStorageActionHandlerTypes()
    {
      @Override
      public TypeReference getTaskType()
      {
        return new TypeReference<Map<String, Integer>>() {};
      }

      @Override
      public TypeReference getTaskStatusType()
      {
        return new TypeReference<Map<String, Integer>>() {};
      }

      @Override
      public TypeReference getTaskActionType()
      {
        return new TypeReference<Map<String, Integer>>() {};
      }

      @Override
      public TypeReference getTaskLockType()
      {
        return new TypeReference<Map<String, Integer>>() {};
      }
    });

    Map<String, Integer> task = ImmutableMap.of("taskId", 1);
    Map<String, Integer> taskStatus = ImmutableMap.of("count", 42);
    Map<String, Integer> taskStatus2 = ImmutableMap.of("count", 42, "temp", 1);

    connector.createTaskTables();

    final String taskId = "1234";

    handler.insert(taskId, new DateTime("2014-01-02T00:00:00.123"), "testDataSource", task, true, taskStatus);

    Assert.assertEquals(
        Optional.of(task),
        handler.getTask(taskId)
    );

    Assert.assertEquals(
        ImmutableList.of(Pair.of(task, taskStatus)),
        handler.getActiveTasksWithStatus()
    );

    Assert.assertTrue(handler.setStatus(taskId, true, taskStatus2));

    Assert.assertEquals(
        ImmutableList.of(Pair.of(task, taskStatus2)),
        handler.getActiveTasksWithStatus()
    );

    Assert.assertEquals(
        ImmutableList.of(),
        handler.getRecentlyFinishedTaskStatuses(new DateTime("2014-01-01"))
    );

    Assert.assertTrue(handler.setStatus(taskId, false, taskStatus));

    Assert.assertEquals(
        Optional.of(taskStatus),
        handler.getTaskStatus(taskId)
    );

    // inactive statuses cannot be updated, this should fail
    Assert.assertFalse(handler.setStatus(taskId, false, taskStatus2));

    Assert.assertEquals(
        Optional.of(taskStatus),
        handler.getTaskStatus(taskId)
    );

    Assert.assertEquals(
        Optional.of(task),
        handler.getTask(taskId)
    );

    Assert.assertEquals(
        ImmutableList.of(),
        handler.getRecentlyFinishedTaskStatuses(new DateTime("2014-01-03"))
    );

    Assert.assertEquals(
        ImmutableList.of(taskStatus),
        handler.getRecentlyFinishedTaskStatuses(new DateTime("2014-01-01"))
    );
  }
}
