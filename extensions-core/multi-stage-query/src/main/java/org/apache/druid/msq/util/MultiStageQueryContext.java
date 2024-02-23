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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.opencsv.RFC4180Parser;
import com.opencsv.RFC4180ParserBuilder;
import org.apache.druid.error.DruidException;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.msq.exec.ClusterStatisticsMergeMode;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.exec.SegmentSource;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.sql.MSQMode;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.IndexSpec;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Class for all MSQ context params
 * <p>
 * One of the design goals for MSQ is to have less turning parameters. If a parameter is not expected to change the result
 * of a job, but only how the job runs, it's a parameter we can skip documenting in external docs.
 * </p>
 * <br></br>
 * List of context parameters not present in external docs:
 * <br></br>
 * <ol>
 *
 * <li><b>maxInputBytesPerWorker</b>: Should be used in conjunction with taskAssignment `auto` mode. When dividing the
 * input of a stage among the workers, this parameter determines the maximum size in bytes that are given to a single worker
 * before the next worker is chosen.This parameter is only used as a guideline during input slicing, and does not guarantee
 * that a the input cannot be larger.
 * <br></br>
 * For example, we have 3 files. 3, 7, 12 GB each. then we would end up using 2 worker: worker 1 -> 3, 7 and worker 2 -> 12.
 * This value is used for all stages in a query. Default valus is: <b>10737418240</b></li>
 *
 * <li><b>clusterStatisticsMergeMode</b>: Whether to use parallel or sequential mode for merging of the worker sketches.
 * Can be <b>PARALLEL</b>, <b>SEQUENTIAL</b> or <b>AUTO</b>. See {@link ClusterStatisticsMergeMode} for more information on each mode.
 * Default value is <b>SEQUENTIAL</b></li>
 *
 * <li><b>selectDestination</b>: If the query is a Select, determines the location to write results to, once the query
 * is finished. Depending on the location, the results might also be truncated to {@link Limits#MAX_SELECT_RESULT_ROWS}.
 * Default value is {@link MSQSelectDestination#TASKREPORT}, which writes all the results to the report.
 *
 * <li><b>useAutoColumnSchemas</b>: Temporary flag to allow experimentation using
 * {@link org.apache.druid.segment.AutoTypeColumnSchema} for all 'standard' type columns during segment generation,
 * see {@link DimensionSchemaUtils#createDimensionSchema} for more details.
 *
 * <li><b>arrayIngestMode</b>: Tri-state query context that controls the behaviour and support of arrays that are
 * ingested via MSQ. If set to 'none', arrays are not allowed to be ingested in MSQ. If set to 'array', array types
 * can be ingested as expected. If set to 'mvd', numeric arrays can not be ingested, and string arrays will be
 * ingested as MVDs (this is kept for legacy purpose).
 *
 * <li><b>taskLockType</b>: Temporary flag to allow MSQ to use experimental lock types. Valid values are present in
 * {@link TaskLockType}. If the flag is not set, msq uses {@link TaskLockType#EXCLUSIVE} for replace queries and
 * {@link TaskLockType#SHARED} for insert queries.
 *
 * </ol>
 **/
public class MultiStageQueryContext
{
  public static final String CTX_MSQ_MODE = "mode";
  public static final String DEFAULT_MSQ_MODE = MSQMode.STRICT_MODE.toString();

  public static final String CTX_MAX_NUM_TASKS = "maxNumTasks";
  @VisibleForTesting
  static final int DEFAULT_MAX_NUM_TASKS = 2;

  public static final String CTX_TASK_ASSIGNMENT_STRATEGY = "taskAssignment";
  private static final String DEFAULT_TASK_ASSIGNMENT_STRATEGY = WorkerAssignmentStrategy.MAX.toString();

  public static final String CTX_FINALIZE_AGGREGATIONS = "finalizeAggregations";
  private static final boolean DEFAULT_FINALIZE_AGGREGATIONS = true;

  public static final String CTX_INCLUDE_SEGMENT_SOURCE = "includeSegmentSource";
  public static final SegmentSource DEFAULT_INCLUDE_SEGMENT_SOURCE = SegmentSource.NONE;

  public static final String CTX_DURABLE_SHUFFLE_STORAGE = "durableShuffleStorage";
  private static final boolean DEFAULT_DURABLE_SHUFFLE_STORAGE = false;
  public static final String CTX_SELECT_DESTINATION = "selectDestination";
  private static final String DEFAULT_SELECT_DESTINATION = MSQSelectDestination.TASKREPORT.getName();

  public static final String CTX_FAULT_TOLERANCE = "faultTolerance";
  public static final boolean DEFAULT_FAULT_TOLERANCE = false;

  public static final String CTX_FAIL_ON_EMPTY_INSERT = "failOnEmptyInsert";
  public static final boolean DEFAULT_FAIL_ON_EMPTY_INSERT = false;

  public static final String CTX_SEGMENT_LOAD_WAIT = "waitUntilSegmentsLoad";
  public static final boolean DEFAULT_SEGMENT_LOAD_WAIT = false;
  public static final String CTX_MAX_INPUT_BYTES_PER_WORKER = "maxInputBytesPerWorker";

  public static final String CTX_CLUSTER_STATISTICS_MERGE_MODE = "clusterStatisticsMergeMode";
  public static final String DEFAULT_CLUSTER_STATISTICS_MERGE_MODE = ClusterStatisticsMergeMode.SEQUENTIAL.toString();

  public static final String CTX_ROWS_PER_SEGMENT = "rowsPerSegment";
  static final int DEFAULT_ROWS_PER_SEGMENT = 3000000;

  public static final String CTX_ROWS_PER_PAGE = "rowsPerPage";
  static final int DEFAULT_ROWS_PER_PAGE = 100000;

  public static final String CTX_ROWS_IN_MEMORY = "rowsInMemory";
  // Lower than the default to minimize the impact of per-row overheads that are not accounted for by
  // OnheapIncrementalIndex. For example: overheads related to creating bitmaps during persist.
  static final int DEFAULT_ROWS_IN_MEMORY = 100000;

  public static final String CTX_IS_REINDEX = "isReindex";

  /**
   * Controls sort order within segments. Normally, this is the same as the overall order of the query (from the
   * CLUSTERED BY clause) but it can be overridden.
   */
  public static final String CTX_SORT_ORDER = "segmentSortOrder";

  public static final String CTX_INDEX_SPEC = "indexSpec";

  public static final String CTX_USE_AUTO_SCHEMAS = "useAutoColumnSchemas";
  public static final boolean DEFAULT_USE_AUTO_SCHEMAS = false;

  public static final String CTX_ARRAY_INGEST_MODE = "arrayIngestMode";
  public static final ArrayIngestMode DEFAULT_ARRAY_INGEST_MODE = ArrayIngestMode.MVD;


  private static final Pattern LOOKS_LIKE_JSON_ARRAY = Pattern.compile("^\\s*\\[.*", Pattern.DOTALL);

  public static String getMSQMode(final QueryContext queryContext)
  {
    return queryContext.getString(
        CTX_MSQ_MODE,
        DEFAULT_MSQ_MODE
    );
  }

  public static boolean isDurableStorageEnabled(final QueryContext queryContext)
  {
    return queryContext.getBoolean(
        CTX_DURABLE_SHUFFLE_STORAGE,
        DEFAULT_DURABLE_SHUFFLE_STORAGE
    );
  }

  public static boolean isFaultToleranceEnabled(final QueryContext queryContext)
  {
    return queryContext.getBoolean(
        CTX_FAULT_TOLERANCE,
        DEFAULT_FAULT_TOLERANCE
    );
  }

  public static boolean isFailOnEmptyInsertEnabled(final QueryContext queryContext)
  {
    return queryContext.getBoolean(
        CTX_FAIL_ON_EMPTY_INSERT,
        DEFAULT_FAIL_ON_EMPTY_INSERT
    );
  }

  public static boolean shouldWaitForSegmentLoad(final QueryContext queryContext)
  {
    return queryContext.getBoolean(
        CTX_SEGMENT_LOAD_WAIT,
        DEFAULT_SEGMENT_LOAD_WAIT
    );
  }

  public static boolean isReindex(final QueryContext queryContext)
  {
    return queryContext.getBoolean(
        CTX_IS_REINDEX,
        true
    );
  }

  public static long getMaxInputBytesPerWorker(final QueryContext queryContext)
  {
    return queryContext.getLong(
        CTX_MAX_INPUT_BYTES_PER_WORKER,
        Limits.DEFAULT_MAX_INPUT_BYTES_PER_WORKER
    );
  }

  public static ClusterStatisticsMergeMode getClusterStatisticsMergeMode(QueryContext queryContext)
  {
    return QueryContexts.getAsEnum(
        CTX_CLUSTER_STATISTICS_MERGE_MODE,
        queryContext.getString(CTX_CLUSTER_STATISTICS_MERGE_MODE, DEFAULT_CLUSTER_STATISTICS_MERGE_MODE),
        ClusterStatisticsMergeMode.class
    );
  }

  public static boolean isFinalizeAggregations(final QueryContext queryContext)
  {
    return queryContext.getBoolean(
        CTX_FINALIZE_AGGREGATIONS,
        DEFAULT_FINALIZE_AGGREGATIONS
    );
  }

  public static SegmentSource getSegmentSources(final QueryContext queryContext)
  {
    return queryContext.getEnum(
        CTX_INCLUDE_SEGMENT_SOURCE,
        SegmentSource.class,
        DEFAULT_INCLUDE_SEGMENT_SOURCE
    );
  }

  public static WorkerAssignmentStrategy getAssignmentStrategy(final QueryContext queryContext)
  {
    return QueryContexts.getAsEnum(
        CTX_TASK_ASSIGNMENT_STRATEGY,
        queryContext.getString(CTX_TASK_ASSIGNMENT_STRATEGY, DEFAULT_TASK_ASSIGNMENT_STRATEGY),
        WorkerAssignmentStrategy.class
    );
  }

  public static int getMaxNumTasks(final QueryContext queryContext)
  {
    return queryContext.getInt(
        CTX_MAX_NUM_TASKS,
        DEFAULT_MAX_NUM_TASKS
    );
  }

  public static int getRowsPerSegment(final QueryContext queryContext)
  {
    return queryContext.getInt(
        CTX_ROWS_PER_SEGMENT,
        DEFAULT_ROWS_PER_SEGMENT
    );
  }

  public static int getRowsPerPage(final QueryContext queryContext)
  {
    return queryContext.getInt(
        CTX_ROWS_PER_PAGE,
        DEFAULT_ROWS_PER_PAGE
    );
  }


  public static MSQSelectDestination getSelectDestination(final QueryContext queryContext)
  {
    return QueryContexts.getAsEnum(
        CTX_SELECT_DESTINATION,
        queryContext.getString(CTX_SELECT_DESTINATION, DEFAULT_SELECT_DESTINATION),
        MSQSelectDestination.class
    );
  }

  @Nullable
  public static MSQSelectDestination getSelectDestinationOrNull(final QueryContext queryContext)
  {
    return QueryContexts.getAsEnum(
        CTX_SELECT_DESTINATION,
        queryContext.getString(CTX_SELECT_DESTINATION),
        MSQSelectDestination.class
    );
  }

  public static int getRowsInMemory(final QueryContext queryContext)
  {
    return queryContext.getInt(CTX_ROWS_IN_MEMORY, DEFAULT_ROWS_IN_MEMORY);
  }

  public static List<String> getSortOrder(final QueryContext queryContext)
  {
    return MultiStageQueryContext.decodeSortOrder(queryContext.getString(CTX_SORT_ORDER));
  }

  @Nullable
  public static IndexSpec getIndexSpec(final QueryContext queryContext, final ObjectMapper objectMapper)
  {
    return decodeIndexSpec(queryContext.get(CTX_INDEX_SPEC), objectMapper);
  }

  public static boolean useAutoColumnSchemas(final QueryContext queryContext)
  {
    return queryContext.getBoolean(CTX_USE_AUTO_SCHEMAS, DEFAULT_USE_AUTO_SCHEMAS);
  }

  public static ArrayIngestMode getArrayIngestMode(final QueryContext queryContext)
  {
    return queryContext.getEnum(CTX_ARRAY_INGEST_MODE, ArrayIngestMode.class, DEFAULT_ARRAY_INGEST_MODE);
  }

  /**
   * Decodes {@link #CTX_SORT_ORDER} from either a JSON or CSV string.
   */
  @Nullable
  @VisibleForTesting
  static List<String> decodeSortOrder(@Nullable final String sortOrderString)
  {
    if (sortOrderString == null) {
      return Collections.emptyList();
    } else if (LOOKS_LIKE_JSON_ARRAY.matcher(sortOrderString).matches()) {
      try {
        // Not caching this ObjectMapper in a static, because we expect to use it infrequently (once per INSERT
        // query that uses this feature) and there is no need to keep it around longer than that.
        return new ObjectMapper().readValue(sortOrderString, new TypeReference<List<String>>()
        {
        });
      }
      catch (JsonProcessingException e) {
        throw QueryContexts.badValueException(CTX_SORT_ORDER, "CSV or JSON array", sortOrderString);
      }
    } else {
      final RFC4180Parser csvParser = new RFC4180ParserBuilder().withSeparator(',').build();

      try {
        return Arrays.stream(csvParser.parseLine(sortOrderString))
                     .filter(s -> s != null && !s.isEmpty())
                     .map(String::trim)
                     .collect(Collectors.toList());
      }
      catch (IOException e) {
        throw QueryContexts.badValueException(CTX_SORT_ORDER, "CSV or JSON array", sortOrderString);
      }
    }
  }

  /**
   * Decodes {@link #CTX_INDEX_SPEC} from either a JSON-encoded string, or POJOs.
   */
  @Nullable
  @VisibleForTesting
  static IndexSpec decodeIndexSpec(@Nullable final Object indexSpecObject, final ObjectMapper objectMapper)
  {
    try {
      if (indexSpecObject == null) {
        return null;
      } else if (indexSpecObject instanceof String) {
        return objectMapper.readValue((String) indexSpecObject, IndexSpec.class);
      } else {
        return objectMapper.convertValue(indexSpecObject, IndexSpec.class);
      }
    }
    catch (Exception e) {
      throw QueryContexts.badValueException(CTX_INDEX_SPEC, "an indexSpec", indexSpecObject);
    }
  }

  /**
   * This method is used to validate and get the taskLockType from the queryContext.
   * If the queryContext does not contain the taskLockType, then {@link TaskLockType#EXCLUSIVE} is used for replace queries and
   * {@link TaskLockType#SHARED} is used for insert queries.
   * If the queryContext contains the taskLockType, then it is validated and returned.
   */
  public static TaskLockType validateAndGetTaskLockType(QueryContext queryContext, boolean isReplaceQuery)
  {
    final boolean useConcurrentLocks = queryContext.getBoolean(
        Tasks.USE_CONCURRENT_LOCKS,
        Tasks.DEFAULT_USE_CONCURRENT_LOCKS
    );
    if (useConcurrentLocks) {
      return isReplaceQuery ? TaskLockType.REPLACE : TaskLockType.APPEND;
    }
    final TaskLockType taskLockType = QueryContexts.getAsEnum(
        Tasks.TASK_LOCK_TYPE,
        queryContext.getString(Tasks.TASK_LOCK_TYPE, null),
        TaskLockType.class
    );
    if (taskLockType == null) {
      if (isReplaceQuery) {
        return TaskLockType.EXCLUSIVE;
      } else {
        return TaskLockType.SHARED;
      }
    }
    final String appendErrorMessage = StringUtils.format(
        " Please use [%s] key in the context parameter and use one of the TaskLock types as mentioned earlier or "
        + "remove this key for automatic lock type selection", Tasks.TASK_LOCK_TYPE);

    if (isReplaceQuery && !(taskLockType.equals(TaskLockType.EXCLUSIVE) || taskLockType.equals(TaskLockType.REPLACE))) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "TaskLock must be of type [%s] or [%s] for a REPLACE query. Found invalid type [%s] set."
                              + appendErrorMessage,
                              TaskLockType.EXCLUSIVE,
                              TaskLockType.REPLACE,
                              taskLockType
                          );
    }
    if (!isReplaceQuery && !(taskLockType.equals(TaskLockType.SHARED) || taskLockType.equals(TaskLockType.APPEND))) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "TaskLock must be of type [%s] or [%s] for an INSERT query. Found invalid type [%s] set."
                              + appendErrorMessage,
                              TaskLockType.SHARED,
                              TaskLockType.APPEND,
                              taskLockType
                          );
    }
    return taskLockType;
  }
}
