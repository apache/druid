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
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.fabric8.kubernetes.api.model.ObjectMeta;
import io.fabric8.kubernetes.api.model.PodTemplate;
import org.apache.druid.indexing.common.task.NoopTask;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TaskPropertiesPodTemplateSelectStrategyTest
{
  private Map<String, PodTemplate> templates;

  @Before
  public void setup()
  {
    templates = ImmutableMap.of(
        "mock",
        new PodTemplate(null, null, new ObjectMeta()
        {
          @Override
          public String getName()
          {
            return "mock";
          }
        }, null),
        "no_match",
        new PodTemplate(null, null, new ObjectMeta()
        {
          @Override
          public String getName()
          {
            return "no_match";
          }
        }, null),
        "match",
        new PodTemplate(null, null, new ObjectMeta()
        {
          @Override
          public String getName()
          {
            return "match";
          }
        }, null),
        "base",
        new PodTemplate(null, "base", new ObjectMeta()
        {
          @Override
          public String getName()
          {
            return "base";
          }
        }, null)
    );
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowNullPointerExceptionWhenTemplateSelectorsAreNull()
  {
    new TaskPropertiesPodTemplateSelectStrategy(null);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowNullPointerExceptionWhenTemplateKeyIsNull()
  {
    new TaskPropertiesPodTemplateSelectStrategy.TemplateSelector(null, task -> false);
  }

  @Test(expected = NullPointerException.class)
  public void shouldThrowNullPointerExceptionWhenMatcherIsNull()
  {
    new TaskPropertiesPodTemplateSelectStrategy.TemplateSelector("myTemplate", null);
  }

  @Test
  public void testGetPodTemplate_ForTask_emptySelectorsFallbackToBaseTemplate()
  {
    List<TaskPropertiesPodTemplateSelectStrategy.TemplateSelector> emptySelectors = Collections.emptyList();
    TaskPropertiesPodTemplateSelectStrategy strategy = new TaskPropertiesPodTemplateSelectStrategy(emptySelectors);
    Task task = NoopTask.create();
    Assert.assertEquals("base", strategy.getPodTemplateForTask(task, templates).getMetadata().getName());
  }

  @Test
  public void testGetPodTemplate_ForTask_noMatchSelectorsFallbackToBaseTemplate()
  {
    TaskPropertiesPodTemplateSelectStrategy.TemplateSelector noMatchSelector = new MockTemplateSelector(false, "mock");
    List<TaskPropertiesPodTemplateSelectStrategy.TemplateSelector> selectors = Collections.singletonList(noMatchSelector);
    TaskPropertiesPodTemplateSelectStrategy strategy = new TaskPropertiesPodTemplateSelectStrategy(selectors);
    Task task = NoopTask.create();
    Assert.assertEquals("base", strategy.getPodTemplateForTask(task, templates).getMetadata().getName());
  }

  @Test
  public void testGetPodTemplate_ForTask_withMatchSelectors()
  {
    TaskPropertiesPodTemplateSelectStrategy.TemplateSelector noMatchSelector = new MockTemplateSelector(
        false,
        "no_match"
    );
    TaskPropertiesPodTemplateSelectStrategy.TemplateSelector matchSelector = new MockTemplateSelector(true, "match");
    List<TaskPropertiesPodTemplateSelectStrategy.TemplateSelector> selectors = Lists.newArrayList(
        noMatchSelector,
        matchSelector
    );
    TaskPropertiesPodTemplateSelectStrategy strategy = new TaskPropertiesPodTemplateSelectStrategy(selectors);
    Task task = NoopTask.create();
    Assert.assertEquals("match", strategy.getPodTemplateForTask(task, templates).getMetadata().getName());
  }

  @Test
  public void testSerde() throws Exception
  {
    final ObjectMapper objectMapper = TestHelper.makeJsonMapper();
    Map<String, Set<String>> cxtTagsConditions = new HashMap<>();
    cxtTagsConditions.put("tag1", Sets.newHashSet("tag1value"));

    Map<String, Set<String>> taskFieldsConditions = new HashMap<>();
    taskFieldsConditions.put("type", Sets.newHashSet(NoopTask.TYPE));

    TaskPropertiesMatcher matcher = new TaskPropertiesMatcher(
        cxtTagsConditions,
        taskFieldsConditions
    );

    TaskPropertiesPodTemplateSelectStrategy strategy = new TaskPropertiesPodTemplateSelectStrategy(Collections.singletonList(
        new TaskPropertiesPodTemplateSelectStrategy.TemplateSelector("TestSelector", matcher)));

    TaskPropertiesPodTemplateSelectStrategy strategy2 = objectMapper.readValue(
        objectMapper.writeValueAsBytes(strategy),
        TaskPropertiesPodTemplateSelectStrategy.class
    );
    Assert.assertEquals(strategy, strategy2);
  }

  static class MockTemplateSelector extends TaskPropertiesPodTemplateSelectStrategy.TemplateSelector
  {
    MockTemplateSelector(boolean matches, String name)
    {
      super(name, task -> matches);
    }
  }
}
