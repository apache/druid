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

package org.apache.druid.k8s.overlord.execution;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.query.DruidMetrics;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SelectorTest
{
  @Test
  public void shouldReturnTrueWhenMatchTasksTagsAndEmptyDataSource()
  {
    Map<String, Set<String>> cxtTagsConditions = new HashMap<>();
    cxtTagsConditions.put("tag1", Sets.newHashSet("tag1Value"));

    Task task = NoopTask.create();
    task.addToContext(DruidMetrics.TAGS, ImmutableMap.of("tag1", "tag1Value"));

    Selector selector = new Selector(
        "TestSelector",
        cxtTagsConditions,
        Sets.newHashSet(NoopTask.TYPE),
        new HashSet<>()
    );

    Assert.assertTrue(selector.evaluate(task));
  }

  @Test
  public void shouldReturnTrueWhenMatchDataSourceTagsAndEmptyTasks()
  {
    String datasource = "table";
    Map<String, Set<String>> cxtTagsConditions = new HashMap<>();
    cxtTagsConditions.put("tag1", Sets.newHashSet("tag1Value"));

    Task task = NoopTask.forDatasource(datasource);
    task.addToContext(DruidMetrics.TAGS, ImmutableMap.of("tag1", "tag1Value"));

    Selector selector = new Selector(
        "TestSelector",
        cxtTagsConditions,
        new HashSet<>(),
        Sets.newHashSet(datasource)
    );

    Assert.assertTrue(selector.evaluate(task));
  }

  @Test
  public void shouldReturnTrueWhenMatchDataSourceTasksAndEmptyTags()
  {
    String datasource = "table";
    Map<String, Set<String>> cxtTagsConditions = new HashMap<>();

    Task task = NoopTask.forDatasource(datasource);

    Selector selector = new Selector(
        "TestSelector",
        cxtTagsConditions,
        Sets.newHashSet(NoopTask.TYPE),
        Sets.newHashSet(datasource)
    );

    Assert.assertTrue(selector.evaluate(task));
  }

  @Test
  public void shouldReturnTrueWhenAllTagsAndTasksMatch()
  {
    String dataSource = "my_table";
    Map<String, Set<String>> cxtTagsConditions = new HashMap<>();
    cxtTagsConditions.put("tag1", Sets.newHashSet("tag1value"));

    Task task = NoopTask.forDatasource(dataSource);
    task.addToContext(DruidMetrics.TAGS, ImmutableMap.of("tag1", "tag1value"));

    Selector selector = new Selector(
        "TestSelector",
        cxtTagsConditions,
        null,
        Sets.newHashSet(dataSource)
    );

    Assert.assertTrue(selector.evaluate(task));
  }

  @Test
  public void shouldReturnFalseWhenTagsDoNotMatch()
  {
    String dataSource = "my_table";
    Map<String, Set<String>> cxtTagsConditions = new HashMap<>();
    cxtTagsConditions.put("nonexistentTag", Sets.newHashSet("tag1value"));

    Task task = NoopTask.forDatasource(dataSource);

    Selector selector = new Selector(
        "TestSelector",
        cxtTagsConditions,
        null,
        Sets.newHashSet(dataSource)
    );

    Assert.assertFalse(selector.evaluate(task));
  }

  @Test
  public void shouldReturnFalseWhenSomeTagsDoNotMatch()
  {
    String dataSource = "my_table";
    Map<String, Set<String>> cxtTagsConditions = new HashMap<>();
    cxtTagsConditions.put("nonexistentTag", Sets.newHashSet("nonexistentTagValue"));
    cxtTagsConditions.put("tag1", Sets.newHashSet("tag1value"));

    Task task = NoopTask.forDatasource(dataSource);
    task.addToContext(DruidMetrics.TAGS, ImmutableMap.of("tag1", "tag1value"));

    Selector selector = new Selector(
        "TestSelector",
        cxtTagsConditions,
        null,
        Sets.newHashSet(dataSource)
    );

    Assert.assertFalse(selector.evaluate(task));
  }

  @Test
  public void shouldReturnFalseWhenTaskFieldsDoNotMatch()
  {
    Map<String, Set<String>> cxtTagsConditions = new HashMap<>();
    cxtTagsConditions.put("tag1", Sets.newHashSet("tag1value"));

    Task task = NoopTask.forDatasource("another_table");
    task.addToContext(DruidMetrics.TAGS, ImmutableMap.of("tag1", "tag1value"));

    Selector selector = new Selector(
        "TestSelector",
        cxtTagsConditions,
        null,
        Sets.newHashSet("my_table")
    );

    Assert.assertFalse(selector.evaluate(task));
  }

  @Test
  public void shouldReturnFalseWhenSomeTaskFieldsDoNotMatch()
  {
    Map<String, Set<String>> cxtTagsConditions = new HashMap<>();
    cxtTagsConditions.put("tag1", Sets.newHashSet("tag1value"));

    Task task = NoopTask.forDatasource("another_table");
    task.addToContext(DruidMetrics.TAGS, ImmutableMap.of("tag1", "tag1value"));

    Selector selector = new Selector(
        "TestSelector",
        cxtTagsConditions,
        Sets.newHashSet(NoopTask.TYPE),
        Sets.newHashSet("my_table")
    );

    Assert.assertFalse(selector.evaluate(task));
  }

  @Test
  public void shouldReturnTrueWhenNoConditionsSpecified()
  {
    Task task = NoopTask.forDatasource("my_table");
    task.addToContext(DruidMetrics.TAGS, ImmutableMap.of("tag1", "tag1value"));

    Selector selector = new Selector(
        "TestSelector",
        null,
        null,
        null
    );

    Assert.assertTrue(selector.evaluate(task));
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Map<String, Set<String>> cxtTagsConditions = new HashMap<>();
    cxtTagsConditions.put("tag1", Sets.newHashSet("tag1value"));

    Selector selector = new Selector(
        "TestSelector",
        cxtTagsConditions,
        Sets.newHashSet(NoopTask.TYPE),
        Sets.newHashSet("my_table")
    );

    Selector selector2 = objectMapper.readValue(
        objectMapper.writeValueAsBytes(selector),
        Selector.class
    );
    Assert.assertEquals(selector, selector2);
  }
}
