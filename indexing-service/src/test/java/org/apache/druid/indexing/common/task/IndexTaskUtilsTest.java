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
import org.apache.druid.java.util.emitter.service.ServiceMetricEvent;
import org.apache.druid.query.DruidMetrics;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Map;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class IndexTaskUtilsTest
{
  private static final Map<String, Object> METRIC_TAGS = ImmutableMap.of("k1", "v1", "k2", 20);

  private static final String GROUP_ID = "groupId123";
  private Task task;
  @Mock
  private AbstractTask abstractTask;
  private ServiceMetricEvent.Builder metricBuilder;

  @BeforeEach
  public void setUp()
  {
    metricBuilder = ServiceMetricEvent.builder();
    task = new NoopTask(GROUP_ID, GROUP_ID, "wiki", 1L, 0L, Map.of(DruidMetrics.TAGS, METRIC_TAGS));
    Mockito.when(abstractTask.getContextValue(DruidMetrics.TAGS)).thenReturn(METRIC_TAGS);
    Mockito.when(abstractTask.getGroupId()).thenReturn(GROUP_ID);
    Mockito.when(abstractTask.getId()).thenReturn(GROUP_ID);
    Mockito.when(abstractTask.getType()).thenReturn("test");
    Mockito.when(abstractTask.getDataSource()).thenReturn("wiki");
    Mockito.when(abstractTask.getIngestionMode()).thenReturn(AbstractTask.IngestionMode.APPEND);
  }

  @Test
  public void testSetTaskDimensionsWithContextTagsShouldSetTags()
  {
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);
    Assertions.assertEquals(METRIC_TAGS, metricBuilder.getDimension(DruidMetrics.TAGS));
  }

  @Test
  public void testSetTaskDimensionsForAbstractTaskWithContextTagsShouldSetTags()
  {
    IndexTaskUtils.setTaskDimensions(metricBuilder, abstractTask);
    Assertions.assertEquals(METRIC_TAGS, metricBuilder.getDimension(DruidMetrics.TAGS));
  }

  @Test
  public void testSetTaskDimensionsWithoutTagsShouldNotSetTags()
  {
    task = new NoopTask(null, null, "wiki", 1L, 0L, null);
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);
    Assertions.assertNull(metricBuilder.getDimension(DruidMetrics.TAGS));
  }

  @Test
  public void testSetTaskDimensionsForAbstractTaskWithoutTagsShouldNotSetTags()
  {
    Mockito.when(abstractTask.getContextValue(DruidMetrics.TAGS)).thenReturn(null);
    IndexTaskUtils.setTaskDimensions(metricBuilder, abstractTask);
    Assertions.assertNull(metricBuilder.getDimension(DruidMetrics.TAGS));
  }

  @Test
  public void testSetTaskDimensionsWithGroupIdShouldSetGroupId()
  {
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);
    Assertions.assertEquals(GROUP_ID, metricBuilder.getDimension(DruidMetrics.GROUP_ID));
  }

  @Test
  public void testSetTaskDimensionsForAbstractTaskWithGroupIdShouldSetGroupId()
  {
    IndexTaskUtils.setTaskDimensions(metricBuilder, abstractTask);
    Assertions.assertEquals(GROUP_ID, metricBuilder.getDimension(DruidMetrics.GROUP_ID));
  }

  @Test
  public void testSetTaskDimensionsForAbstractTaskWithoutGroupIdShouldNotSetGroupId()
  {
    Mockito.when(abstractTask.getGroupId()).thenReturn(null);
    IndexTaskUtils.setTaskDimensions(metricBuilder, abstractTask);
    Assertions.assertNull(metricBuilder.getDimension(DruidMetrics.GROUP_ID));
  }
}
