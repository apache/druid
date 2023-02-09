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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class IndexTaskUtilsTest
{
  private static final Map<String, Object> METRIC_TAGS = ImmutableMap.of("k1", "v1", "k2", 20);
  @Mock
  private Task task;
  @Mock
  private AbstractTask abstractTask;
  private ServiceMetricEvent.Builder metricBuilder;

  @Before
  public void setUp()
  {
    metricBuilder = ServiceMetricEvent.builder();
    Mockito.when(task.getContextValue(DruidMetrics.TAGS)).thenReturn(METRIC_TAGS);
    Mockito.when(abstractTask.getContextValue(DruidMetrics.TAGS)).thenReturn(METRIC_TAGS);
  }

  @Test
  public void testSetTaskDimensionsWithContextTagsShouldSetTags()
  {
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);
    Assert.assertEquals(METRIC_TAGS, metricBuilder.getDimension(DruidMetrics.TAGS));
  }

  @Test
  public void testSetTaskDimensionsForAbstractTaskWithContextTagsShouldSetTags()
  {
    IndexTaskUtils.setTaskDimensions(metricBuilder, abstractTask);
    Assert.assertEquals(METRIC_TAGS, metricBuilder.getDimension(DruidMetrics.TAGS));
  }

  @Test
  public void testSetTaskDimensionsWithoutTagsShouldNotSetTags()
  {
    Mockito.when(task.getContextValue(DruidMetrics.TAGS)).thenReturn(null);
    IndexTaskUtils.setTaskDimensions(metricBuilder, task);
    Assert.assertNull(metricBuilder.getDimension(DruidMetrics.TAGS));
  }

  @Test
  public void testSetTaskDimensionsForAbstractTaskWithoutTagsShouldNotSetTags()
  {
    Mockito.when(abstractTask.getContextValue(DruidMetrics.TAGS)).thenReturn(null);
    IndexTaskUtils.setTaskDimensions(metricBuilder, abstractTask);
    Assert.assertNull(metricBuilder.getDimension(DruidMetrics.TAGS));
  }
}
