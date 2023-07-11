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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.druid.indexer.TaskLocation;
import org.apache.druid.indexer.TaskStatus;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.worker.TaskAnnouncement;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.function.Function;

public class ZkWorkerTest
{
  Function<ChildData, String> extract;

  @Before
  public void setup()
  {
    ObjectMapper mapper = new DefaultObjectMapper();
    extract = ZkWorker.createTaskIdExtractor(mapper);
  }

  ChildData prepare(String input)
  {
    String replaced = StringUtils.format(StringUtils.replaceChar(input, '\'', "\""), TaskAnnouncement.TASK_ID_KEY);
    byte[] data = StringUtils.toUtf8(replaced);
    return new ChildData("/a/b/c", new Stat(), data);
  }

  @Test
  public void testShallowObjectWithIdFirst()
  {
    ChildData input = prepare("{'%s': 'abcd', 'status': 'RUNNING'}");
    String actual = extract.apply(input);
    Assert.assertEquals("abcd", actual);
  }

  @Test
  public void testShallowObjectWithIdMiddle()
  {
    ChildData input = prepare("{'before': 'something', '%s': 'abcd', 'status': 'RUNNING'}");
    String actual = extract.apply(input);
    Assert.assertEquals("abcd", actual);
  }

  @Test
  public void testShallowObjectWithIdLast()
  {
    ChildData input = prepare("{'before': 'something', 'status': 'RUNNING', '%s': 'abcd'}");
    String actual = extract.apply(input);
    Assert.assertEquals("abcd", actual);
  }

  @Test
  public void testShallowObjectWithNoId()
  {
    ChildData input = prepare("{'before': 'something', 'status': 'RUNNING'}");
    String actual = extract.apply(input);
    Assert.assertNull(actual);
  }

  @Test
  public void testDeepObjectWithIdFirst()
  {
    ChildData input = prepare("{'%s': 'abcd', 'subobject': { 'subkey': 'subvalue' }, 'subarray': [{'key': 'val'}, 2, 3], 'status': 'RUNNING'}");
    String actual = extract.apply(input);
    Assert.assertEquals("abcd", actual);
  }

  @Test
  public void testDeepObjectWithIdLast()
  {
    ChildData input = prepare("{'subobject': { 'subkey': 'subvalue' }, 'subarray': [{'key': 'val'}, 2, 3], 'status': 'RUNNING', '%s': 'abcd'}");
    String actual = extract.apply(input);
    Assert.assertEquals("abcd", actual);
  }

  @Test
  public void testDeepObjectWithIdInNestedOnly()
  {
    ChildData input = prepare("{'subobject': { '%s': 'defg' }, 'subarray': [{'key': 'val'}, 2, 3], 'status': 'RUNNING'}");
    String actual = extract.apply(input);
    Assert.assertNull(actual);
  }

  @Test
  public void testDeepObjectWithIdInNestedAndOuter()
  {
    ChildData input = prepare("{'subobject': { '%s': 'defg' }, 'subarray': [{'key': 'val'}, 2, 3], 'status': 'RUNNING', '%1$s': 'abcd'}");
    String actual = extract.apply(input);
    Assert.assertEquals("abcd", actual);
  }

  @Test
  public void testIdWithWrongTypeReturnsNull()
  {
    ChildData input = prepare("{'%s': {'nested': 'obj'}'");
    String actual = extract.apply(input);
    Assert.assertNull(actual);
  }

  @Test
  public void testCanReadIdFromAJacksonSerializedTaskAnnouncement() throws JsonProcessingException
  {
    final String expectedTaskId = "task01234";

    Task task0 = NoopTask.create(expectedTaskId, 0);
    TaskAnnouncement taskAnnouncement = TaskAnnouncement.create(
            task0,
            TaskStatus.running(task0.getId()),
            TaskLocation.unknown()
    );

    ObjectMapper objectMapper = new ObjectMapper();

    byte[] serialized = objectMapper.writeValueAsBytes(taskAnnouncement);

    ChildData zkNode = new ChildData("/a/b/c", new Stat(), serialized);

    String actualExtractedTaskId = extract.apply(zkNode);
    Assert.assertEquals(expectedTaskId, actualExtractedTaskId);
  }
}
