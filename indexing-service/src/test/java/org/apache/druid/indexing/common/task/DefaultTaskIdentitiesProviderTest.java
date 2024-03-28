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

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(MockitoJUnitRunner.class)
public class DefaultTaskIdentitiesProviderTest
{
  private Task task;
  private DefaultTaskIdentitiesProvider provider;

  @Before
  public void setup()
  {
    provider = new DefaultTaskIdentitiesProvider();
  }

  @Test
  public void testReturnDefaultTaskIdentifierWhenGroupIdIsNull()
  {
    task = new NoopTask("id", null, "datasource", 0, 0, null);
    provider.enrichTaskTags(task);
    Map<String, Object> tags = task.getContextValue(DruidMetrics.TAGS);
    assertEquals(task.getType(), tags.get(TaskIdentitiesProvider.TASK_IDENTIFIER));
  }

  @Test
  public void testReturnCompactTaskIdentifierWhenGroupIdStartsWithCoordinatorIssuedCompact()
  {
    task = new NoopTask("id", "coordinator-issued_compact_random_id", "datasource", 0, 0, null);
    provider.enrichTaskTags(task);
    Map<String, Object> tags = task.getContextValue(DruidMetrics.TAGS);
    assertEquals("compact", tags.get(TaskIdentitiesProvider.TASK_IDENTIFIER));
  }

  @Test
  public void testReturnKillTaskIdentifierWhenGroupIdStartsWithCoordinatorIssuedKill()
  {
    task = new NoopTask("id", "coordinator-issued_kill_random_id", "datasource", 0, 0, null);
    provider.enrichTaskTags(task);
    Map<String, Object> tags = task.getContextValue(DruidMetrics.TAGS);
    assertEquals("kill", tags.get(TaskIdentitiesProvider.TASK_IDENTIFIER));
  }

  @Test
  public void testAppendTaskIdentifierGetTaskMetricTags()
  {
    Map<String, Object> inputTags = ImmutableMap.of("tag1", "value1", "tag2", "value2");
    task = new NoopTask("id", null, "datasource", 0, 0, ImmutableMap.of(DruidMetrics.TAGS, inputTags));

    provider.enrichTaskTags(task);
    Map<String, Object> tags = task.getContextValue(DruidMetrics.TAGS);

    assertEquals(3, tags.size());
    assertEquals("noop", tags.get(TaskIdentitiesProvider.TASK_IDENTIFIER));
  }
}
