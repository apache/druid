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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DynamicTaskExecutionBehaviorStrategyTest
{

  @Test
  public void testGetTaskCategory_nullSelectors()
  {
    DynamicTaskExecutionBehaviorStrategy strategy = new DynamicTaskExecutionBehaviorStrategy(null);
    Task task = NoopTask.create();
    Assert.assertNull(strategy.getTaskCategory(task));
  }

  @Test
  public void testGetTaskCategory_emptySelectors()
  {
    List<Selector> emptySelectors = Collections.emptyList();
    DynamicTaskExecutionBehaviorStrategy strategy = new DynamicTaskExecutionBehaviorStrategy(emptySelectors);
    Task task = NoopTask.create();
    Assert.assertNull(strategy.getTaskCategory(task));
  }

  @Test
  public void testGetTaskCategory_noMatchSelectors()
  {
    Selector noMatchSelector = new MockSelector(false, "mock");
    List<Selector> selectors = Collections.singletonList(noMatchSelector);
    DynamicTaskExecutionBehaviorStrategy strategy = new DynamicTaskExecutionBehaviorStrategy(selectors);
    Task task = NoopTask.create();
    Assert.assertNull(strategy.getTaskCategory(task));
  }

  @Test
  public void testGetTaskCategory_withMatchSelectors()
  {
    Selector noMatchSelector = new MockSelector(false, "no_match");
    Selector matchSelector = new MockSelector(true, "match");
    List<Selector> selectors = Lists.newArrayList(noMatchSelector, matchSelector);
    DynamicTaskExecutionBehaviorStrategy strategy = new DynamicTaskExecutionBehaviorStrategy(selectors);
    Task task = NoopTask.create();
    Assert.assertEquals("match", strategy.getTaskCategory(task)
    );
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Map<String, Set<String>> cxtTagsConditions = new HashMap<>();
    cxtTagsConditions.put("tag1", Sets.newHashSet("tag1value"));

    Map<String, Set<String>> taskFieldsConditions = new HashMap<>();
    taskFieldsConditions.put("type", Sets.newHashSet(NoopTask.TYPE));

    Selector selector = new Selector(
        "TestSelector",
        cxtTagsConditions,
        taskFieldsConditions
    );

    DynamicTaskExecutionBehaviorStrategy strategy = new DynamicTaskExecutionBehaviorStrategy(Collections.singletonList(
        selector));

    DynamicTaskExecutionBehaviorStrategy strategy2 = objectMapper.readValue(
        objectMapper.writeValueAsBytes(strategy),
        DynamicTaskExecutionBehaviorStrategy.class
    );
    Assert.assertEquals(strategy, strategy2);
  }

  static class MockSelector extends Selector
  {
    private final boolean matches;

    MockSelector(boolean matches, String name)
    {
      super(name, null, null);
      this.matches = matches;
    }

    @Override
    public boolean evaluate(final Task task)
    {
      return matches;
    }
  }
}
