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
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.FrameType;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.NilQueryCounterSnapshot;
import org.apache.druid.msq.exec.ClusterStatisticsMergeMode;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.exec.SegmentSource;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.rpc.ControllerResource;
import org.apache.druid.msq.rpc.SketchEncoding;
import org.apache.druid.msq.sql.MSQMode;
import org.apache.druid.msq.sql.MSQTaskQueryMaker;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.segment.IndexSpec;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
 * <li><b>maxRowsMaterializedInWindow</b>: Query context that specifies the largest window size that can be processed
 * using window functions in MSQ. This is to ensure guardrails using window function in MSQ.
 *
 * </ol>
 **/
public class MultiStageQueryContext
{
  private static final Logger log = new Logger(MultiStageQueryContext.class);

  public static final String CTX_MSQ_MODE = "mode";
  public static final String DEFAULT_MSQ_MODE = MSQMode.STRICT_MODE.toString();

  // Note: CTX_MAX_NUM_TASKS and DEFAULT_MAX_NUM_TASKS values used here should be kept in sync with those in
  // org.apache.druid.client.indexing.ClientMsqContext
  public static final String CTX_MAX_NUM_TASKS = "maxNumTasks";
  @VisibleForTesting
  static final int DEFAULT_MAX_NUM_TASKS = 2;

  public static final String CTX_TASK_ASSIGNMENT_STRATEGY = "taskAssignment";
  private static final String DEFAULT_TASK_ASSIGNMENT_STRATEGY = WorkerAssignmentStrategy.MAX.toString();

  public static final String CTX_FINALIZE_AGGREGATIONS = "finalizeAggregations";
  private static final boolean DEFAULT_FINALIZE_AGGREGATIONS = true;

  public static final String CTX_INCLUDE_SEGMENT_SOURCE = "includeSegmentSource";

  public static final String CTX_MAX_CONCURRENT_STAGES = "maxConcurrentStages";
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

  public static final String CTX_SKETCH_ENCODING_MODE = "sketchEncoding";
  public static final String DEFAULT_CTX_SKETCH_ENCODING_MODE = SketchEncoding.OCTET_STREAM.toString();

  public static final String CTX_ROWS_PER_SEGMENT = "rowsPerSegment";
  public static final int DEFAULT_ROWS_PER_SEGMENT = 3000000;

  public static final String CTX_ROWS_PER_PAGE = "rowsPerPage";
  public static final int DEFAULT_ROWS_PER_PAGE = 100000;

  public static final String CTX_REMOVE_NULL_BYTES = "removeNullBytes";
  public static final boolean DEFAULT_REMOVE_NULL_BYTES = false;

  public static final String CTX_ROWS_IN_MEMORY = "rowsInMemory";
  // Lower than the default to minimize the impact of per-row overheads that are not accounted for by
  // OnheapIncrementalIndex. For example: overheads related to creating bitmaps during persist.
  public static final int DEFAULT_ROWS_IN_MEMORY = 100000;

  public static final String CTX_IS_REINDEX = "isReindex";

  public static final String CTX_MAX_NUM_SEGMENTS = "maxNumSegments";

  public static final String CTX_START_TIME = "startTime";

  /**
   * Controls sort order within segments. Normally, this is the same as the overall order of the query (from the
   * CLUSTERED BY clause) but it can be overridden.
   */
  public static final String CTX_SORT_ORDER = "segmentSortOrder";

  public static final String CTX_INDEX_SPEC = "indexSpec";

  public static final String CTX_USE_AUTO_SCHEMAS = "useAutoColumnSchemas";
  public static final boolean DEFAULT_USE_AUTO_SCHEMAS = false;

  public static final String CTX_ARRAY_INGEST_MODE = "arrayIngestMode";
  public static final ArrayIngestMode DEFAULT_ARRAY_INGEST_MODE = ArrayIngestMode.ARRAY;

  /**
   * Whether new counters (anything other than channel, sortProgress, warnings, segmentGenerationProgress) should
   * be included in reports. This parameter is necessary because prior to Druid 31, we lacked
   * {@link NilQueryCounterSnapshot} as a default counter, which means that {@link SqlStatementResourceHelper} and
   * {@link ControllerResource#httpPostCounters} would throw errors when encountering new counter types that they do
   * not yet recognize. This causes problems during rolling updates.
   *
   * For a rolling update from a version prior to Druid 31, this must be set to "false" by the job submitter.
   */
  public static final String CTX_INCLUDE_ALL_COUNTERS = "includeAllCounters";
  public static final boolean DEFAULT_INCLUDE_ALL_COUNTERS = true;

  public static final String CTX_FORCE_TIME_SORT = DimensionsSpec.PARAMETER_FORCE_TIME_SORT;
  private static final boolean DEFAULT_FORCE_TIME_SORT = DimensionsSpec.DEFAULT_FORCE_TIME_SORT;

  /**
   * The {@link FrameType} to use for row-based frames. This context parameter exists to support rolling updates from
   * older Druid versions. The latest type is given by {@link FrameType#latestRowBased()}, which is set in
   * {@link MSQTaskQueryMaker#buildOverrideContext} starting in Druid 34. Once all servers are on Druid 34 or newer,
   * the current-latest type {@link FrameType#ROW_BASED_V2} is used.
   */
  public static final String CTX_ROW_BASED_FRAME_TYPE = "rowBasedFrameType";
  private static final FrameType DEFAULT_ROW_BASED_FRAME_TYPE = FrameType.ROW_BASED_V1;

  public static final String MAX_ROWS_MATERIALIZED_IN_WINDOW = "maxRowsMaterializedInWindow";

  // This flag ensures backward compatibility and will be removed in Druid 33, with the default behavior as enabled.
  public static final String WINDOW_FUNCTION_OPERATOR_TRANSFORMATION = "windowFunctionOperatorTransformation";

  public static final String CTX_SKIP_TYPE_VERIFICATION = "skipTypeVerification";

  /**
   * Number of partitions to target per worker when creating shuffle specs that involve specific numbers of
   * partitions. This helps us utilize more parallelism when workers are multi-threaded.
   */
  public static final String CTX_TARGET_PARTITIONS_PER_WORKER = "targetPartitionsPerWorker";

  /**
   * Maximum size of frames to create. Defaults to {@link WorkerMemoryParameters#DEFAULT_FRAME_SIZE}.
   */
  public static final String CTX_MAX_FRAME_SIZE = "maxFrameSize";

  /**
   * Maximum number of threads to use for processing. Acts as a cap on the value of {@link WorkerContext#threadCount()}.
   */
  public static final String CTX_MAX_THREADS = "maxThreads";

  private static final Pattern LOOKS_LIKE_JSON_ARRAY = Pattern.compile("^\\s*\\[.*", Pattern.DOTALL);

  public static String getMSQMode(final QueryContext queryContext)
  {
    return queryContext.getString(
        CTX_MSQ_MODE,
        DEFAULT_MSQ_MODE
    );
  }

  public static int getMaxRowsMaterializedInWindow(final QueryContext queryContext)
  {
    return queryContext.getInt(
        MAX_ROWS_MATERIALIZED_IN_WINDOW,
        Limits.MAX_ROWS_MATERIALIZED_IN_WINDOW
    );
  }

  public static boolean isWindowFunctionOperatorTransformationEnabled(final QueryContext queryContext)
  {
    return queryContext.getBoolean(
        WINDOW_FUNCTION_OPERATOR_TRANSFORMATION,
        false
    );
  }

  public static int getMaxConcurrentStagesWithDefault(
      final QueryContext queryContext,
      final int defaultMaxConcurrentStages
  )
  {
    return queryContext.getInt(
        CTX_MAX_CONCURRENT_STAGES,
        defaultMaxConcurrentStages
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
        false
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

  public static SketchEncoding getSketchEncoding(QueryContext queryContext)
  {
    return QueryContexts.getAsEnum(
        CTX_SKETCH_ENCODING_MODE,
        queryContext.getString(CTX_SKETCH_ENCODING_MODE, DEFAULT_CTX_SKETCH_ENCODING_MODE),
        SketchEncoding.class
    );
  }

  public static boolean isFinalizeAggregations(final QueryContext queryContext)
  {
    return queryContext.getBoolean(
        CTX_FINALIZE_AGGREGATIONS,
        DEFAULT_FINALIZE_AGGREGATIONS
    );
  }

  public static SegmentSource getSegmentSources(final QueryContext queryContext, final SegmentSource defaultSource)
  {
    return queryContext.getEnum(
        CTX_INCLUDE_SEGMENT_SOURCE,
        SegmentSource.class,
        defaultSource
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

  public static boolean removeNullBytes(final QueryContext queryContext)
  {
    return queryContext.getBoolean(CTX_REMOVE_NULL_BYTES, DEFAULT_REMOVE_NULL_BYTES);
  }

  public static boolean isDartQuery(final QueryContext queryContext)
  {
    return queryContext.get(QueryContexts.CTX_DART_QUERY_ID) != null;
  }

  public static MSQSelectDestination getSelectDestination(final QueryContext queryContext)
  {
    MSQSelectDestination destination = QueryContexts.getAsEnum(
        CTX_SELECT_DESTINATION,
        queryContext.getString(CTX_SELECT_DESTINATION, DEFAULT_SELECT_DESTINATION),
        MSQSelectDestination.class
    );

    if (isDartQuery(queryContext)) {
      if (!MSQSelectDestination.TASKREPORT.equals(destination)) {
        log.warn(
            "Dart does not support [%s]. Using [%s] instead.",
            destination,
            MSQSelectDestination.TASKREPORT
        );
      }
      return MSQSelectDestination.TASKREPORT;
    }
    return destination;
  }

  public static int getRowsInMemory(final QueryContext queryContext)
  {
    return queryContext.getInt(CTX_ROWS_IN_MEMORY, DEFAULT_ROWS_IN_MEMORY);
  }

  public static Integer getMaxNumSegments(final QueryContext queryContext)
  {
    // The default is null, if the context is not set.
    return queryContext.getInt(CTX_MAX_NUM_SEGMENTS);
  }

  public static List<String> getSortOrder(final QueryContext queryContext)
  {
    return decodeList(CTX_SORT_ORDER, queryContext.getString(CTX_SORT_ORDER));
  }

  @Nullable
  public static IndexSpec getIndexSpec(final QueryContext queryContext, final ObjectMapper objectMapper)
  {
    return decodeIndexSpec(queryContext.get(CTX_INDEX_SPEC), objectMapper);
  }

  public static long getMaxParseExceptions(final QueryContext queryContext)
  {
    return queryContext.getLong(
        MSQWarnings.CTX_MAX_PARSE_EXCEPTIONS_ALLOWED,
        MSQWarnings.DEFAULT_MAX_PARSE_EXCEPTIONS_ALLOWED
    );
  }

  public static boolean useAutoColumnSchemas(final QueryContext queryContext)
  {
    return queryContext.getBoolean(CTX_USE_AUTO_SCHEMAS, DEFAULT_USE_AUTO_SCHEMAS);
  }

  public static ArrayIngestMode getArrayIngestMode(final QueryContext queryContext)
  {
    return queryContext.getEnum(CTX_ARRAY_INGEST_MODE, ArrayIngestMode.class, DEFAULT_ARRAY_INGEST_MODE);
  }

  public static int getTargetPartitionsPerWorkerWithDefault(
      final QueryContext queryContext,
      final int defaultValue
  )
  {
    return queryContext.getInt(CTX_TARGET_PARTITIONS_PER_WORKER, defaultValue);
  }

  /**
   * See {@link #CTX_INCLUDE_ALL_COUNTERS}.
   */
  public static boolean getIncludeAllCounters(final QueryContext queryContext)
  {
    return queryContext.getBoolean(CTX_INCLUDE_ALL_COUNTERS, DEFAULT_INCLUDE_ALL_COUNTERS);
  }

  public static boolean isForceSegmentSortByTime(final QueryContext queryContext)
  {
    return queryContext.getBoolean(CTX_FORCE_TIME_SORT, DEFAULT_FORCE_TIME_SORT);
  }

  /**
   * Returns the value of {@link #CTX_ROW_BASED_FRAME_TYPE}, or {@link #DEFAULT_ROW_BASED_FRAME_TYPE}.
   *
   * @see #CTX_ROW_BASED_FRAME_TYPE for more details
   */
  public static FrameType getRowBasedFrameType(final QueryContext queryContext)
  {
    return FrameType.forVersion(
        (byte) queryContext.getInt(
            CTX_ROW_BASED_FRAME_TYPE,
            DEFAULT_ROW_BASED_FRAME_TYPE.version()
        )
    );
  }

  public static DateTime getStartTime(final QueryContext queryContext)
  {
    // Get the start time from the query context set by the broker.
    if (!queryContext.containsKey(CTX_START_TIME)) {
      // If it is missing, as could be the case for an older version of the broker, use the current time instead, to
      // have something to timeout against.
      DateTime startTime = DateTimes.nowUtc();
      log.warn("Query context does not contain start time. Defaulting to the current time[%s] instead.", startTime);
      return startTime;
    }
    return DateTimes.of(queryContext.getString(CTX_START_TIME));
  }

  public static Set<String> getColumnsExcludedFromTypeVerification(final QueryContext queryContext)
  {
    return new HashSet<>(decodeList(CTX_SKIP_TYPE_VERIFICATION, queryContext.getString(CTX_SKIP_TYPE_VERIFICATION)));
  }

  public static int getFrameSize(final QueryContext queryContext)
  {
    return queryContext.getInt(CTX_MAX_FRAME_SIZE, WorkerMemoryParameters.DEFAULT_FRAME_SIZE);
  }

  public static Integer getMaxThreads(final QueryContext queryContext)
  {
    return queryContext.getInt(CTX_MAX_THREADS);
  }

  /**
   * Decodes a list from either a JSON or CSV string.
   */
  @VisibleForTesting
  static List<String> decodeList(final String keyName, @Nullable final String listString)
  {
    if (listString == null) {
      return Collections.emptyList();
    } else if (LOOKS_LIKE_JSON_ARRAY.matcher(listString).matches()) {
      try {
        // Not caching this ObjectMapper in a static, because we expect to use it infrequently (once per INSERT
        // query that uses this feature) and there is no need to keep it around longer than that.
        return new ObjectMapper().readValue(listString, new TypeReference<>() {});
      }
      catch (JsonProcessingException e) {
        throw QueryContexts.badValueException(keyName, "CSV or JSON array", listString);
      }
    } else {
      final RFC4180Parser csvParser = new RFC4180ParserBuilder().withSeparator(',').build();

      try {
        return Arrays.stream(csvParser.parseLine(listString))
                     .filter(s -> s != null && !s.isEmpty())
                     .map(String::trim)
                     .collect(Collectors.toList());
      }
      catch (IOException e) {
        throw QueryContexts.badValueException(keyName, "CSV or JSON array", listString);
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
