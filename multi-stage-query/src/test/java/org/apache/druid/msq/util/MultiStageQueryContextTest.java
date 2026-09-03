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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.ThrowableMatcher;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.query.BadQueryContextException;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.column.StringEncodingStrategy;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_ARRAY_INGEST_MODE;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_DURABLE_SHUFFLE_STORAGE;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_FAULT_TOLERANCE;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_FINALIZE_AGGREGATIONS;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_MAX_FRAME_SIZE;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_MAX_NUM_TASKS;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_MAX_ROWS_IN_MEMORY;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_MAX_THREADS;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_MSQ_MODE;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_REMOVE_NULL_BYTES;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_ROWS_IN_MEMORY;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_ROWS_PER_SEGMENT;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_SORT_ORDER;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_TASK_ASSIGNMENT_STRATEGY;
import static org.apache.druid.msq.util.MultiStageQueryContext.CTX_USE_AUTO_SCHEMAS;
import static org.apache.druid.msq.util.MultiStageQueryContext.DEFAULT_MAX_NUM_TASKS;

public class MultiStageQueryContextTest
{
  @Test
  public void isDurableShuffleStorageEnabled_unset_returnsDefaultValue()
  {
    Assertions.assertFalse(MultiStageQueryContext.isDurableStorageEnabled(QueryContext.empty()));
  }

  @Test
  public void isDurableShuffleStorageEnabled_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_DURABLE_SHUFFLE_STORAGE, "true");
    Assertions.assertTrue(MultiStageQueryContext.isDurableStorageEnabled(QueryContext.of(propertyMap)));
  }

  @Test
  public void isFaultToleranceEnabled_unset_returnsDefaultValue()
  {
    Assertions.assertFalse(MultiStageQueryContext.isFaultToleranceEnabled(QueryContext.empty()));
  }

  @Test
  public void isFaultToleranceEnabled_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_FAULT_TOLERANCE, "true");
    Assertions.assertTrue(MultiStageQueryContext.isFaultToleranceEnabled(QueryContext.of(propertyMap)));
  }

  @Test
  public void isFinalizeAggregations_unset_returnsDefaultValue()
  {
    Assertions.assertTrue(MultiStageQueryContext.isFinalizeAggregations(QueryContext.empty()));
  }

  @Test
  public void isFinalizeAggregations_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_FINALIZE_AGGREGATIONS, "false");
    Assertions.assertFalse(MultiStageQueryContext.isFinalizeAggregations(QueryContext.of(propertyMap)));
  }

  @Test
  public void getAssignmentStrategy_unset_returnsDefaultValue()
  {
    Assertions.assertEquals(
        WorkerAssignmentStrategy.MAX,
        MultiStageQueryContext.getAssignmentStrategy(QueryContext.empty())
    );
  }

  @Test
  public void getMaxInputBytesPerWorker_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(MultiStageQueryContext.CTX_MAX_INPUT_BYTES_PER_WORKER, 1024);

    Assertions.assertEquals(
        1024,
        MultiStageQueryContext.getMaxInputBytesPerWorker(QueryContext.of(propertyMap))
    );
  }

  @Test
  public void getMaxInputFilesPerWorker_unset_returnsDefaultValue()
  {
    Assertions.assertEquals(
        Limits.DEFAULT_MAX_INPUT_FILES_PER_WORKER,
        MultiStageQueryContext.getMaxInputFilesPerWorker(QueryContext.empty())
    );
  }

  @Test
  public void getMaxInputFilesPerWorker_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(MultiStageQueryContext.CTX_MAX_INPUT_FILES_PER_WORKER, 5000);
    Assertions.assertEquals(5000, MultiStageQueryContext.getMaxInputFilesPerWorker(QueryContext.of(propertyMap)));
  }

  @Test
  public void getMaxInputFilesPerWorker_zero_throwsException()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(MultiStageQueryContext.CTX_MAX_INPUT_FILES_PER_WORKER, 0);
    Assertions.assertThrows(
        DruidException.class,
        () -> MultiStageQueryContext.getMaxInputFilesPerWorker(QueryContext.of(propertyMap))
    );
  }

  @Test
  public void getMaxInputFilesPerWorker_negative_throwsException()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(MultiStageQueryContext.CTX_MAX_INPUT_FILES_PER_WORKER, -1);
    Assertions.assertThrows(
        DruidException.class,
        () -> MultiStageQueryContext.getMaxInputFilesPerWorker(QueryContext.of(propertyMap))
    );
  }

  @Test
  public void getMaxPartitions_unset_returnsDefaultValue()
  {
    Assertions.assertEquals(
        Limits.DEFAULT_MAX_PARTITIONS,
        MultiStageQueryContext.getMaxPartitions(QueryContext.empty())
    );
  }

  @Test
  public void getMaxPartitions_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(MultiStageQueryContext.CTX_MAX_PARTITIONS, 50000);
    Assertions.assertEquals(50000, MultiStageQueryContext.getMaxPartitions(QueryContext.of(propertyMap)));
  }

  @Test
  public void getMaxPartitions_zero_throwsException()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(MultiStageQueryContext.CTX_MAX_PARTITIONS, 0);
    Assertions.assertThrows(
        DruidException.class,
        () -> MultiStageQueryContext.getMaxPartitions(QueryContext.of(propertyMap))
    );
  }

  @Test
  public void getMaxPartitions_negative_throwsException()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(MultiStageQueryContext.CTX_MAX_PARTITIONS, -1);
    Assertions.assertThrows(
        DruidException.class,
        () -> MultiStageQueryContext.getMaxPartitions(QueryContext.of(propertyMap))
    );
  }

  @Test
  public void getAssignmentStrategy_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_TASK_ASSIGNMENT_STRATEGY, "AUTO");
    Assertions.assertEquals(
        WorkerAssignmentStrategy.AUTO,
        MultiStageQueryContext.getAssignmentStrategy(QueryContext.of(propertyMap))
    );
  }

  @Test
  public void getMaxNumTasks_unset_returnsDefaultValue()
  {
    Assertions.assertEquals(DEFAULT_MAX_NUM_TASKS, MultiStageQueryContext.getMaxNumTasks(QueryContext.empty()));
  }

  @Test
  public void getMaxNumTasks_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_MAX_NUM_TASKS, 101);
    Assertions.assertEquals(101, MultiStageQueryContext.getMaxNumTasks(QueryContext.of(propertyMap)));
  }

  @Test
  public void getRowsPerSegment_unset_returnsDefaultValue()
  {
    Assertions.assertEquals(
        MultiStageQueryContext.DEFAULT_ROWS_PER_SEGMENT,
        MultiStageQueryContext.getRowsPerSegment(QueryContext.empty())
    );
  }

  @Test
  public void getRowsPerSegment_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ROWS_PER_SEGMENT, 10);
    Assertions.assertEquals(10, MultiStageQueryContext.getRowsPerSegment(QueryContext.of(propertyMap)));
  }

  @Test
  public void getMaxRowsInMemory_unset_returnsDefaultValue()
  {
    Assertions.assertEquals(
        MultiStageQueryContext.DEFAULT_MAX_ROWS_IN_MEMORY,
        MultiStageQueryContext.getMaxRowsInMemory(QueryContext.empty())
    );
  }

  @Test
  public void getMaxRowsInMemory_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_MAX_ROWS_IN_MEMORY, 10);
    Assertions.assertEquals(10, MultiStageQueryContext.getMaxRowsInMemory(QueryContext.of(propertyMap)));
  }

  @Test
  public void getMaxRowsInMemory_altSet_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ROWS_IN_MEMORY, 20);
    Assertions.assertEquals(20, MultiStageQueryContext.getMaxRowsInMemory(QueryContext.of(propertyMap)));
  }

  @Test
  public void getMaxRowsInMemory_bothSet_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_ROWS_IN_MEMORY, 20, CTX_MAX_ROWS_IN_MEMORY, 10);
    Assertions.assertEquals(10, MultiStageQueryContext.getMaxRowsInMemory(QueryContext.of(propertyMap)));
  }

  @Test
  public void getSortOrder_unset_returnsDefaultValue()
  {
    Assertions.assertEquals(Collections.emptyList(), MultiStageQueryContext.getSortOrder(QueryContext.empty()));
  }

  @Test
  public void getSortOrder_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_SORT_ORDER, "a, b,\"c,d\"");
    Assertions.assertEquals(
        ImmutableList.of("a", "b", "c,d"),
        MultiStageQueryContext.getSortOrder(QueryContext.of(propertyMap))
    );
  }

  @Test
  public void getMSQMode_unset_returnsDefaultValue()
  {
    Assertions.assertEquals("strict", MultiStageQueryContext.getMSQMode(QueryContext.empty()));
  }

  @Test
  public void getMSQMode_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_MSQ_MODE, "nonStrict");
    Assertions.assertEquals("nonStrict", MultiStageQueryContext.getMSQMode(QueryContext.of(propertyMap)));
  }

  @Test
  public void getSelectDestination_unset_returnsDefaultValue()
  {
    Assertions.assertEquals(
        MSQSelectDestination.TASKREPORT,
        MultiStageQueryContext.getSelectDestination(QueryContext.empty())
    );
  }

  @Test
  public void useAutoColumnSchemes_unset_returnsDefaultValue()
  {
    Assertions.assertFalse(MultiStageQueryContext.useAutoColumnSchemas(QueryContext.empty()));
  }

  @Test
  public void useAutoColumnSchemes_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_USE_AUTO_SCHEMAS, true);
    Assertions.assertTrue(MultiStageQueryContext.useAutoColumnSchemas(QueryContext.of(propertyMap)));
  }

  @Test
  public void arrayIngestMode_unset_returnsDefaultValue()
  {
    Assertions.assertEquals(ArrayIngestMode.ARRAY, MultiStageQueryContext.getArrayIngestMode(QueryContext.empty()));
  }

  @Test
  public void arrayIngestMode_set_returnsCorrectValue()
  {
    Assertions.assertEquals(
        ArrayIngestMode.MVD,
        MultiStageQueryContext.getArrayIngestMode(QueryContext.of(ImmutableMap.of(CTX_ARRAY_INGEST_MODE, "mvd")))
    );

    Assertions.assertEquals(
        ArrayIngestMode.ARRAY,
        MultiStageQueryContext.getArrayIngestMode(QueryContext.of(ImmutableMap.of(CTX_ARRAY_INGEST_MODE, "array")))
    );

    Assertions.assertThrows(
        BadQueryContextException.class,
        () ->
            MultiStageQueryContext.getArrayIngestMode(QueryContext.of(ImmutableMap.of(CTX_ARRAY_INGEST_MODE, "dummy")))
    );
  }

  @Test
  public void removeNullBytes_unset_returnsDefaultValue()
  {
    Assertions.assertFalse(MultiStageQueryContext.removeNullBytes(QueryContext.empty()));
  }

  @Test
  public void removeNullBytes_set_returnsCorrectValue()
  {
    Assertions.assertTrue(
        MultiStageQueryContext.removeNullBytes(QueryContext.of(ImmutableMap.of(CTX_REMOVE_NULL_BYTES, true)))
    );

    Assertions.assertFalse(
        MultiStageQueryContext.removeNullBytes(QueryContext.of(ImmutableMap.of(CTX_REMOVE_NULL_BYTES, false)))
    );
  }

  @Test
  public void testDecodeSortOrder()
  {
    Assertions.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder("a, b,\"c,d\""));
    Assertions.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder(" a, b,\"c,d\""));
    Assertions.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder("[\"a\", \"b\", \"c,d\"]"));
    Assertions.assertEquals(ImmutableList.of("a", "b", "c,d"), decodeSortOrder(" [\"a\", \"b\", \"c,d\"] "));
    Assertions.assertEquals(ImmutableList.of(), decodeSortOrder("[]"));
    Assertions.assertEquals(ImmutableList.of(), decodeSortOrder(""));
    Assertions.assertEquals(ImmutableList.of(), decodeSortOrder(null));

    Assertions.assertThrows(BadQueryContextException.class, () -> decodeSortOrder("[["));
  }

  @Test
  public void testGetIndexSpec()
  {
    Assertions.assertNull(decodeIndexSpec(null));

    Assertions.assertEquals(IndexSpec.getDefault(), decodeIndexSpec("{}"));
    Assertions.assertEquals(IndexSpec.getDefault(), decodeIndexSpec(Collections.emptyMap()));

    Assertions.assertEquals(
        IndexSpec.builder()
                 .withStringDictionaryEncoding(new StringEncodingStrategy.FrontCoded(null, null))
                 .build(),
        decodeIndexSpec("{\"stringDictionaryEncoding\":{\"type\":\"frontCoded\"}}")
    );

    Assertions.assertEquals(
        IndexSpec.builder()
                 .withStringDictionaryEncoding(new StringEncodingStrategy.FrontCoded(null))
                 .build(),
        decodeIndexSpec(ImmutableMap.of("stringDictionaryEncoding", ImmutableMap.of("type", "frontCoded")))
    );

    final BadQueryContextException e = Assertions.assertThrows(
        BadQueryContextException.class,
        () -> decodeIndexSpec("{")
    );

    ThrowableMatcher.of(BadQueryContextException.class)
                   .expectMessageIs("Expected key [indexSpec] to be an indexSpec, but got [{]")
                   .assertThat(e);
  }

  @Test
  public void testUseConcurrentLocks()
  {
    final QueryContext context = QueryContext.of(ImmutableMap.of(Tasks.USE_CONCURRENT_LOCKS, true));

    Assertions.assertEquals(
        TaskLockType.REPLACE,
        MultiStageQueryContext.validateAndGetTaskLockType(context, true)
    );

    Assertions.assertEquals(
        TaskLockType.APPEND,
        MultiStageQueryContext.validateAndGetTaskLockType(context, false)
    );
  }

  @Test
  public void testDartSelectDestination()
  {
    final QueryContext context = QueryContext.builder()
        .put(QueryContextParameters.DART_QUERY_ID, "test")
        .putRaw(MultiStageQueryContext.CTX_SELECT_DESTINATION, "durablestorage")
        .toContext();

    Assertions.assertEquals(
        MSQSelectDestination.TASKREPORT,
        MultiStageQueryContext.getSelectDestination(context)
    );
  }

  @Test
  public void getFrameSize_unset_returnsDefaultValue()
  {
    Assertions.assertEquals(
        WorkerMemoryParameters.DEFAULT_FRAME_SIZE,
        MultiStageQueryContext.getFrameSize(QueryContext.empty())
    );
  }

  @Test
  public void getFrameSize_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_MAX_FRAME_SIZE, 500000);
    Assertions.assertEquals(500000, MultiStageQueryContext.getFrameSize(QueryContext.of(propertyMap)));
  }

  @Test
  public void getMaxThreads_unset_returnsNull()
  {
    Assertions.assertNull(MultiStageQueryContext.getMaxThreads(QueryContext.empty()));
  }

  @Test
  public void getMaxThreads_set_returnsCorrectValue()
  {
    Map<String, Object> propertyMap = ImmutableMap.of(CTX_MAX_THREADS, 4);
    Assertions.assertEquals(Integer.valueOf(4), MultiStageQueryContext.getMaxThreads(QueryContext.of(propertyMap)));
  }

  @Test
  public void getVirtualStoragePartialDownloadsEnabled_unset_returnsDefault()
  {
    Assertions.assertFalse(MultiStageQueryContext.getVirtualStoragePartialDownloadsEnabled(QueryContext.empty(), false));
    Assertions.assertTrue(MultiStageQueryContext.getVirtualStoragePartialDownloadsEnabled(QueryContext.empty(), true));
  }

  @Test
  public void getVirtualStoragePartialDownloadsEnabled_set_overridesDefault()
  {
    Assertions.assertTrue(
        MultiStageQueryContext.getVirtualStoragePartialDownloadsEnabled(
            QueryContext.of(ImmutableMap.of(MultiStageQueryContext.CTX_VIRTUAL_STORAGE_PARTIAL_DOWNLOADS, true)),
            false
        )
    );
    Assertions.assertFalse(
        MultiStageQueryContext.getVirtualStoragePartialDownloadsEnabled(
            QueryContext.of(ImmutableMap.of(MultiStageQueryContext.CTX_VIRTUAL_STORAGE_PARTIAL_DOWNLOADS, false)),
            true
        )
    );
  }

  @Test
  public void withCommonContext_noTimeout_setsStartTimeOnly()
  {
    final QueryContext context = MultiStageQueryContext.withCommonContext(QueryContext.empty());
    Assertions.assertTrue(context.containsKey(MultiStageQueryContext.CTX_START_TIME));
    Assertions.assertFalse(context.containsKey(MultiStageQueryContext.CTX_QUERY_DEADLINE));
    Assertions.assertEquals(true, context.get(QueryContextParameters.FINALIZE.getName()));
    Assertions.assertEquals(true, context.get(MultiStageQueryContext.WINDOW_FUNCTION_OPERATOR_TRANSFORMATION));
    Assertions.assertTrue(context.containsKey(MultiStageQueryContext.CTX_ROW_BASED_FRAME_TYPE));
  }

  @Test
  public void withCommonContext_withTimeout_setsDeadline()
  {
    final long timeoutMs = 60_000;
    final QueryContext context = MultiStageQueryContext.withCommonContext(
        QueryContext.of(QueryContextParameters.TIMEOUT, timeoutMs)
    );

    Assertions.assertTrue(context.containsKey(MultiStageQueryContext.CTX_START_TIME));
    Assertions.assertTrue(context.containsKey(MultiStageQueryContext.CTX_QUERY_DEADLINE));

    final DateTime startTime = DateTimes.of((String) context.get(MultiStageQueryContext.CTX_START_TIME));
    final DateTime deadline = DateTimes.of((String) context.get(MultiStageQueryContext.CTX_QUERY_DEADLINE));
    Assertions.assertEquals(timeoutMs, deadline.getMillis() - startTime.getMillis());
  }

  @Test
  public void withCommonContext_mergesUserContext()
  {
    final QueryContext context = MultiStageQueryContext.withCommonContext(
        QueryContext.of(ImmutableMap.of("customKey", "customValue"))
    );

    Assertions.assertEquals("customValue", context.get("customKey"));
  }

  private static List<String> decodeSortOrder(@Nullable final String input)
  {
    return MultiStageQueryContext.decodeList(MultiStageQueryContext.CTX_SORT_ORDER, input);
  }

  private static IndexSpec decodeIndexSpec(@Nullable final Object inputSpecObject)
  {
    return MultiStageQueryContext.decodeIndexSpec(inputSpecObject, new ObjectMapper());
  }
}
