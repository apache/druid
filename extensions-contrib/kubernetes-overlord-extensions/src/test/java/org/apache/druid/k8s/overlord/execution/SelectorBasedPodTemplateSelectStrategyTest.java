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
import org.apache.druid.k8s.overlord.taskadapter.PodTemplateWithName;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SelectorBasedPodTemplateSelectStrategyTest
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
  public void shouldThrowNullPointerExceptionWhenSelectorsAreNull()
  {
    new SelectorBasedPodTemplateSelectStrategy(null);
  }

  @Test
  public void testGetPodTemplate_ForTask_emptySelectorsFallbackToBaseTemplate()
  {
    List<Selector> emptySelectors = Collections.emptyList();
    SelectorBasedPodTemplateSelectStrategy strategy = new SelectorBasedPodTemplateSelectStrategy(emptySelectors);
    Task task = NoopTask.create();
    PodTemplateWithName podTemplateWithName = strategy.getPodTemplateForTask(task, templates);
    Assert.assertEquals("base", podTemplateWithName.getName());
    Assert.assertEquals("base", podTemplateWithName.getPodTemplate().getMetadata().getName());

  }

  @Test
  public void testGetPodTemplate_ForTask_noMatchSelectorsFallbackToBaseTemplateIfNullDefaultKey()
  {
    Selector noMatchSelector = new MockSelector(false, "mock");
    List<Selector> selectors = Collections.singletonList(noMatchSelector);
    SelectorBasedPodTemplateSelectStrategy strategy = new SelectorBasedPodTemplateSelectStrategy(selectors);
    Task task = NoopTask.create();
    PodTemplateWithName podTemplateWithName = strategy.getPodTemplateForTask(task, templates);
    Assert.assertEquals("base", podTemplateWithName.getName());
    Assert.assertEquals("base", podTemplateWithName.getPodTemplate().getMetadata().getName());
  }

  @Test
  public void testGetPodTemplate_ForTask_withMatchSelectors()
  {
    Selector noMatchSelector = new MockSelector(
        false,
        "no_match"
    );
    Selector matchSelector = new MockSelector(true, "match");
    List<Selector> selectors = Lists.newArrayList(
        noMatchSelector,
        matchSelector
    );
    SelectorBasedPodTemplateSelectStrategy strategy = new SelectorBasedPodTemplateSelectStrategy(selectors);
    Task task = NoopTask.create();
    PodTemplateWithName podTemplateWithName = strategy.getPodTemplateForTask(task, templates);
    Assert.assertEquals("match", podTemplateWithName.getName());
    Assert.assertEquals("match", podTemplateWithName.getPodTemplate().getMetadata().getName());
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

    SelectorBasedPodTemplateSelectStrategy strategy = new SelectorBasedPodTemplateSelectStrategy(
        Collections.singletonList(selector));

    SelectorBasedPodTemplateSelectStrategy strategy2 = objectMapper.readValue(
        objectMapper.writeValueAsBytes(strategy),
        SelectorBasedPodTemplateSelectStrategy.class
    );
    Assert.assertEquals(strategy, strategy2);
  }

  static class MockSelector extends Selector
  {
    private final boolean matches;

    MockSelector(boolean matches, String name)
    {
      super(name, null, null, null);
      this.matches = matches;
    }

    @Override
    public boolean evaluate(final Task task)
    {
      return matches;
    }
  }
}
