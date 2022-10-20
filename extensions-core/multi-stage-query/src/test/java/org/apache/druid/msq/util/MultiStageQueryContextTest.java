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

package org.apache.druid.msq.util;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.query.QueryContext;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_DESTINATION;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_ENABLE_DURABLE_SHUFFLE_STORAGE;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_FINALIZE_AGGREGATIONS;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_MAX_NUM_TASKS;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_MSQ_MODE;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_ROWS_IN_MEMORY;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_ROWS_PER_SEGMENT;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_SORT_ORDER;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_TASK_ASSIGNMENT_STRATEGY;
import static org.apache.druid.msq.util.MultiStageQueryContext.DEFAULT_MAX_NUM_TASKS;

public class MultiStageQueryContextTest
{
  @Test
  public void isDurableStorageEnabled_noParameterSetReturnsDefaultValue()
  {
    Assert.assertFalse(MultiStageQueryContext.isDurableStorageEnabled(QueryContext.empty()));
  }

  @Test
  public void isDurableStorageEnabled_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ENABLE_DURABLE_SHUFFLE_STORAGE, "true");
    Assert.assertTrue(MultiStageQueryContext.isDurableStorageEnabled(QueryContext.of(propertyMap)));
  }

  @Test
  public void isFinalizeAggregations_noParameterSetReturnsDefaultValue()
  {
    Assert.assertTrue(MultiStageQueryContext.isFinalizeAggregations(QueryContext.empty()));
  }

  @Test
  public void isFinalizeAggregations_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_FINALIZE_AGGREGATIONS, "false");
    Assert.assertFalse(MultiStageQueryContext.isFinalizeAggregations(QueryContext.of(propertyMap)));
  }

  @Test
  public void getAssignmentStrategy_noParameterSetReturnsDefaultValue()
  {
    Assert.assertEquals(WorkerAssignmentStrategy.MAX, MultiStageQueryContext.getAssignmentStrategy(QueryContext.empty()));
  }

  @Test
  public void getAssignmentStrategy_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_TASK_ASSIGNMENT_STRATEGY, "AUTO");
    Assert.assertEquals(
        WorkerAssignmentStrategy.AUTO,
        MultiStageQueryContext.getAssignmentStrategy(QueryContext.of(propertyMap))
    );
  }

  @Test
  public void getMaxNumTasks_noParameterSetReturnsDefaultValue()
  {
    Assert.assertEquals(DEFAULT_MAX_NUM_TASKS, MultiStageQueryContext.getMaxNumTasks(QueryContext.empty()));
  }

  @Test
  public void getMaxNumTasks_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_MAX_NUM_TASKS, 101);
    Assert.assertEquals(101, MultiStageQueryContext.getMaxNumTasks(QueryContext.of(propertyMap)));
  }

  @Test
  public void getMaxNumTasks_legacyParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_MAX_NUM_TASKS, 101);
    Assert.assertEquals(101, MultiStageQueryContext.getMaxNumTasks(QueryContext.of(propertyMap)));
  }

  @Test
  public void getDestination_noParameterSetReturnsDefaultValue()
  {
    Assert.assertNull(MultiStageQueryContext.getDestination(QueryContext.empty()));
  }

  @Test
  public void getDestination_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_DESTINATION, "dataSource");
    Assert.assertEquals("dataSource", MultiStageQueryContext.getDestination(QueryContext.of(propertyMap)));
  }

  @Test
  public void getRowsPerSegment_noParameterSetReturnsDefaultValue()
  {
    Assert.assertEquals(1000, MultiStageQueryContext.getRowsPerSegment(QueryContext.empty(), 1000));
  }

  @Test
  public void getRowsPerSegment_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ROWS_PER_SEGMENT, 10);
    Assert.assertEquals(10, MultiStageQueryContext.getRowsPerSegment(QueryContext.of(propertyMap), 1000));
  }

  @Test
  public void getRowsInMemory_noParameterSetReturnsDefaultValue()
  {
    Assert.assertEquals(1000, MultiStageQueryContext.getRowsInMemory(QueryContext.empty(), 1000));
  }

  @Test
  public void getRowsInMemory_parameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ROWS_IN_MEMORY, 10);
    Assert.assertEquals(10, MultiStageQueryContext.getRowsInMemory(QueryContext.of(propertyMap), 1000));
  }

  @Test
  public void testDecodeSortOrder()
  {
    Assert.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder("a, b,\"c,d\""));
    Assert.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder(" a, b,\"c,d\""));
    Assert.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder("[\"a\", \"b\", \"c,d\"]"));
    Assert.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder(" [\"a\", \"b\", \"c,d\"] "));
    Assert.assertEquals(ImmutableList.of(), decodeSortOrder("[]"));
    Assert.assertEquals(ImmutableList.of(), decodeSortOrder(""));
    Assert.assertEquals(ImmutableList.of(), decodeSortOrder(null));

    Assert.assertThrows(IllegalArgumentException.class, () -> decodeSortOrder("[["));
  }

  @Test
  public void getSortOrderNoParameterSetReturnsDefaultValue()
  {
    Assert.assertNull(MultiStageQueryContext.getSortOrder(QueryContext.empty()));
  }

  @Test
  public void getSortOrderParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_SORT_ORDER, "a, b,\"c,d\"");
    Assert.assertEquals("a, b,\"c,d\"", MultiStageQueryContext.getSortOrder(QueryContext.of(propertyMap)));
  }

  @Test
  public void getMSQModeNoParameterSetReturnsDefaultValue()
  {
    Assert.assertEquals("strict", MultiStageQueryContext.getMSQMode(QueryContext.empty()));
  }

  @Test
  public void getMSQModeParameterSetReturnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_MSQ_MODE, "nonStrict");
    Assert.assertEquals("nonStrict", MultiStageQueryContext.getMSQMode(QueryContext.of(propertyMap)));
  }

  private static List<String> decodeSortOrder(@Nullable final String input)
  {
    return MultiStageQueryContext.decodeSortOrder(input);
  }
}
