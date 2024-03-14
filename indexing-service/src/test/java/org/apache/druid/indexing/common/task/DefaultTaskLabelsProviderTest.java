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

package org.apache.druid.indexing.common.task;


import com.google.common.collect.ImmutableMap;
import org.apache.druid.query.DruidMetrics;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(MockitoJUnitRunner.class)
public class DefaultTaskLabelsProviderTest
{
  private Task task;
  private DefaultTaskLabelsProvider provider;

  @Before
  public void setup()
  {
    provider = new DefaultTaskLabelsProvider();
  }

  @Test
  public void testGetTaskLabels()
  {
    List<String> inputLabels = Arrays.asList("label1", "label2");
    TaskLabel taskLabel = new TaskLabel(inputLabels);
    task = new NoopTask("id", null, "datasource", 0, 0, ImmutableMap.of(Tasks.TASK_LABEL, taskLabel));

    List<String> taskLabels = provider.getTaskLabels(task);

    assertEquals(inputLabels, taskLabels);

    // Test case where no specific task labels are defined within task's context
    task = NoopTask.create();
    taskLabels = provider.getTaskLabels(task);

    assertTrue(taskLabels.size() == 1);
    assertEquals(task.getType(), taskLabels.get(0));
  }

  @Test
  public void testGetTaskMetricTags()
  {
    Map<String, Object> inputTags = ImmutableMap.of("tag1", "value1", "tag2", "value2");
    task = new NoopTask("id", null, "datasource", 0, 0, ImmutableMap.of(DruidMetrics.TAGS, inputTags));

    Map<String, Object> tags = provider.getTaskMetricTags(task);

    assertEquals(inputTags, tags);
  }
}
