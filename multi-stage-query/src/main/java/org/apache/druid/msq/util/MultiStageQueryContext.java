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
import org.apache.druid.error.InvalidInput;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.processor.FrameCombiner;
import org.apache.druid.indexing.common.TaskLockType;
import org.apache.druid.indexing.common.task.Tasks;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.counters.NilQueryCounterSnapshot;
import org.apache.druid.msq.exec.ClusterStatisticsMergeMode;
import org.apache.druid.msq.exec.ExecutionContext;
import org.apache.druid.msq.exec.Limits;
import org.apache.druid.msq.exec.SegmentSource;
import org.apache.druid.msq.exec.StageProcessor;
import org.apache.druid.msq.exec.WorkerMemoryParameters;
import org.apache.druid.msq.indexing.destination.MSQSelectDestination;
import org.apache.druid.msq.indexing.error.MSQWarnings;
import org.apache.druid.msq.kernel.WorkerAssignmentStrategy;
import org.apache.druid.msq.querykit.ReadableInputQueue;
import org.apache.druid.msq.rpc.ControllerResource;
import org.apache.druid.msq.rpc.SketchEncoding;
import org.apache.druid.msq.sql.MSQMode;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.QueryContextParameter;
import org.apache.druid.query.context.QueryContextParameters;
import org.apache.druid.segment.IndexSpec;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Class for all MSQ context params
 * <p>
 * One of the design goals for MSQ is to have less turning parameters. If a parameter is not expected to change the result
 * of a job, but only how the job runs, it's a parameter we can skip documenting in external docs.
 * </p>
 * <br>
 * List of context parameters not present in external docs:
 * <br>
 * <ol>
 *
 * <li><b>maxInputBytesPerWorker</b>: Should be used in conjunction with taskAssignment `auto` mode. When dividing the
 * input of a stage among the workers, this parameter determines the maximum size in bytes that are given to a single worker
 * before the next worker is chosen.This parameter is only used as a guideline during input slicing, and does not guarantee
 * that a the input cannot be larger.
 * <br>
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

  /**
   * @deprecated Use {@link QueryContextParameters#MSQ_MODE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_MSQ_MODE = "mode";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#MSQ_MODE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String DEFAULT_MSQ_MODE = MSQMode.STRICT_MODE.toString();
  /**
   * @deprecated Use {@link QueryContextParameters#MAX_NUM_TASKS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_MAX_NUM_TASKS = "maxNumTasks";
  /**
   * @deprecated Use {@link QueryContextParameters#TASK_ASSIGNMENT_STRATEGY} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_TASK_ASSIGNMENT_STRATEGY = "taskAssignment";
  /**
   * @deprecated Use {@link QueryContextParameters#FINALIZE_AGGREGATIONS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_FINALIZE_AGGREGATIONS = "finalizeAggregations";
  /**
   * @deprecated Use {@link QueryContextParameters#INCLUDE_SEGMENT_SOURCE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_INCLUDE_SEGMENT_SOURCE = "includeSegmentSource";
  /**
   * @deprecated Use {@link QueryContextParameters#MAX_CONCURRENT_STAGES} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_MAX_CONCURRENT_STAGES = "maxConcurrentStages";
  /**
   * @deprecated Use {@link QueryContextParameters#DURABLE_SHUFFLE_STORAGE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_DURABLE_SHUFFLE_STORAGE = "durableShuffleStorage";
  /**
   * @deprecated Use {@link QueryContextParameters#SELECT_DESTINATION} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SELECT_DESTINATION = "selectDestination";
  /**
   * @deprecated Use {@link QueryContextParameters#FAULT_TOLERANCE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_FAULT_TOLERANCE = "faultTolerance";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#FAULT_TOLERANCE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_FAULT_TOLERANCE = false;
  /**
   * @deprecated Use {@link QueryContextParameters#FAIL_ON_EMPTY_INSERT} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_FAIL_ON_EMPTY_INSERT = "failOnEmptyInsert";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#FAIL_ON_EMPTY_INSERT} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_FAIL_ON_EMPTY_INSERT = false;
  /**
   * @deprecated Use {@link QueryContextParameters#WAIT_UNTIL_SEGMENTS_LOAD} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SEGMENT_LOAD_WAIT = "waitUntilSegmentsLoad";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#WAIT_UNTIL_SEGMENTS_LOAD} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_SEGMENT_LOAD_WAIT = false;
  /**
   * @deprecated Use {@link QueryContextParameters#MAX_INPUT_BYTES_PER_WORKER} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_MAX_INPUT_BYTES_PER_WORKER = "maxInputBytesPerWorker";
  /**
   * @deprecated Use {@link QueryContextParameters#MAX_INPUT_FILES_PER_WORKER} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_MAX_INPUT_FILES_PER_WORKER = "maxInputFilesPerWorker";
  /**
   * @deprecated Use {@link QueryContextParameters#MAX_PARTITIONS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_MAX_PARTITIONS = "maxPartitions";
  /**
   * Used by {@link #getMaxClusteredByColumns(QueryContext)}. Can be used to adjust the limit of columns appearing
   * in clusterBy, where the default is {@link Limits#MAX_CLUSTERED_BY_COLUMNS}. This parameter is undocumented
   * because its main purpose is to speed up the test {@code MSQFaultsTest#testInsertWithHugeClusteringKeys}.
   * @deprecated Use {@link QueryContextParameters#MAX_CLUSTERED_BY_COLUMNS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_MAX_CLUSTERED_BY_COLUMNS = "maxClusteredByColumns";
  /**
   * @deprecated Use {@link QueryContextParameters#CLUSTER_STATISTICS_MERGE_MODE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_CLUSTER_STATISTICS_MERGE_MODE = "clusterStatisticsMergeMode";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#CLUSTER_STATISTICS_MERGE_MODE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String DEFAULT_CLUSTER_STATISTICS_MERGE_MODE = ClusterStatisticsMergeMode.SEQUENTIAL.toString();
  /**
   * @deprecated Use {@link QueryContextParameters#SKETCH_ENCODING} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SKETCH_ENCODING_MODE = "sketchEncoding";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#SKETCH_ENCODING} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String DEFAULT_CTX_SKETCH_ENCODING_MODE = SketchEncoding.OCTET_STREAM.toString();
  /**
   * @deprecated Use {@link QueryContextParameters#ROWS_PER_SEGMENT} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_ROWS_PER_SEGMENT = "rowsPerSegment";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#ROWS_PER_SEGMENT} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final int DEFAULT_ROWS_PER_SEGMENT = 3000000;
  /**
   * @deprecated Use {@link QueryContextParameters#ROWS_PER_PAGE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_ROWS_PER_PAGE = "rowsPerPage";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#ROWS_PER_PAGE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final int DEFAULT_ROWS_PER_PAGE = 100000;
  /**
   * @deprecated Use {@link QueryContextParameters#REMOVE_NULL_BYTES} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_REMOVE_NULL_BYTES = "removeNullBytes";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#REMOVE_NULL_BYTES} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_REMOVE_NULL_BYTES = false;
  /**
   * @deprecated Use {@link QueryContextParameters#BACKGROUND_FETCH_EXTERNAL_FILES} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_BACKGROUND_FETCH_EXTERNAL_FILES = "backgroundFetchExternalFiles";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#BACKGROUND_FETCH_EXTERNAL_FILES} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_BACKGROUND_FETCH_EXTERNAL_FILES = true;
  /**
   * Hint to {@link StageProcessor} implementations about whether they should attempt to use
   * {@link FrameCombiner} when doing sort-based aggregations.
   * @deprecated Use {@link QueryContextParameters#USE_COMBINER} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_USE_COMBINER = "useCombiner";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#USE_COMBINER} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_USE_COMBINER = false;
  /**
   * Lower than the default to minimize the impact of per-row overheads that are not accounted for by
   * OnheapIncrementalIndex. For example: overheads related to creating bitmaps during persist.
   * @deprecated Use the default value of {@link QueryContextParameters#MAX_ROWS_IN_MEMORY} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final int DEFAULT_MAX_ROWS_IN_MEMORY = 100000;
  /**
   * @deprecated Use {@link QueryContextParameters#IS_REINDEX} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_IS_REINDEX = "isReindex";
  /**
   * @deprecated Use {@link QueryContextParameters#MAX_NUM_SEGMENTS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_MAX_NUM_SEGMENTS = "maxNumSegments";
  /**
   * @deprecated Use {@link QueryContextParameters#START_TIME} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_START_TIME = "startTime";
  /**
   * The time that a query should time out. Value is an ISO8601 timestamp.
   * @deprecated Use {@link QueryContextParameters#QUERY_DEADLINE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_QUERY_DEADLINE = "queryDeadline";
  /**
   * Controls sort order within segments. Normally, this is the same as the overall order of the query (from the
   * CLUSTERED BY clause) but it can be overridden.
   * @deprecated Use {@link QueryContextParameters#SEGMENT_SORT_ORDER} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SORT_ORDER = "segmentSortOrder";
  /**
   * @deprecated Use {@link QueryContextParameters#INDEX_SPEC} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_INDEX_SPEC = "indexSpec";
  /**
   * @deprecated Use {@link QueryContextParameters#USE_AUTO_COLUMN_SCHEMAS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_USE_AUTO_SCHEMAS = "useAutoColumnSchemas";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#USE_AUTO_COLUMN_SCHEMAS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_USE_AUTO_SCHEMAS = false;
  /**
   * @deprecated Use {@link QueryContextParameters#ARRAY_INGEST_MODE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_ARRAY_INGEST_MODE = "arrayIngestMode";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#ARRAY_INGEST_MODE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final ArrayIngestMode DEFAULT_ARRAY_INGEST_MODE = ArrayIngestMode.ARRAY;
  /**
   * Whether new counters (anything other than channel, sortProgress, warnings, segmentGenerationProgress) should
   * be included in reports. This parameter is necessary because prior to Druid 31, we lacked
   * {@link NilQueryCounterSnapshot} as a default counter, which means that {@link SqlStatementResourceHelper} and
   * {@link ControllerResource#httpPostCounters} would throw errors when encountering new counter types that they do
   * not yet recognize. This causes problems during rolling updates.
   *
   * For a rolling update from a version prior to Druid 31, this must be set to "false" by the job submitter.
   * @deprecated Use {@link QueryContextParameters#INCLUDE_ALL_COUNTERS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_INCLUDE_ALL_COUNTERS = "includeAllCounters";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#INCLUDE_ALL_COUNTERS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final boolean DEFAULT_INCLUDE_ALL_COUNTERS = true;
  /**
   * @deprecated Use {@link QueryContextParameters#VIRTUAL_STORAGE_PARTIAL_DOWNLOADS_ENABLED} instead.
   */
  @Deprecated(since = "40.0.0", forRemoval = true)
  public static final String CTX_VIRTUAL_STORAGE_PARTIAL_DOWNLOADS = Tasks.VIRTUAL_STORAGE_PARTIAL_DOWNLOADS_KEY;

  /**
   * Whether workers should send live counter updates to the controller via the message relay. When enabled, workers
   * periodically send counter snapshots to the controller, allowing the controller to have more up-to-date progress
   * information.
   * @deprecated Use {@link QueryContextParameters#LIVE_REPORT_COUNTERS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_LIVE_REPORT_COUNTERS = "liveReportCounters";
  /**
   * @deprecated Use the default value of {@link QueryContextParameters#FORCE_SEGMENT_SORT_BY_TIME} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_FORCE_TIME_SORT = DimensionsSpec.PARAMETER_FORCE_TIME_SORT;
  /**
   * The {@link FrameType} to use for row-based frames. This context parameter exists to support rolling updates from
   * older Druid versions. The latest type is given by {@link FrameType#latestRowBased()}, which is set in
   * {@link MultiStageQueryContext#withCommonContext} starting in Druid 34. Once all servers are on Druid 34 or newer,
   * the current-latest type {@link FrameType#ROW_BASED_V2} is used.
   * @deprecated Use {@link QueryContextParameters#ROW_BASED_FRAME_TYPE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_ROW_BASED_FRAME_TYPE = "rowBasedFrameType";
  /**
   * @deprecated Use {@link QueryContextParameters#MAX_ROWS_MATERIALIZED_IN_WINDOW} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String MAX_ROWS_MATERIALIZED_IN_WINDOW = "maxRowsMaterializedInWindow";
  /**
   * @deprecated Use {@link QueryContextParameters#WINDOW_FUNCTION_OPERATOR_TRANSFORMATION} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String WINDOW_FUNCTION_OPERATOR_TRANSFORMATION = "windowFunctionOperatorTransformation";
  /**
   * @deprecated Use {@link QueryContextParameters#SKIP_TYPE_VERIFICATION} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SKIP_TYPE_VERIFICATION = "skipTypeVerification";
  /**
   * Number of partitions to target per worker when creating shuffle specs that involve specific numbers of
   * partitions. This helps us utilize more parallelism when workers are multi-threaded.
   * @deprecated Use {@link QueryContextParameters#TARGET_PARTITIONS_PER_WORKER} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_TARGET_PARTITIONS_PER_WORKER = "targetPartitionsPerWorker";
  /**
   * Maximum size of frames to create. Defaults to {@link WorkerMemoryParameters#DEFAULT_FRAME_SIZE}.
   * @deprecated Use {@link QueryContextParameters#MAX_FRAME_SIZE} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_MAX_FRAME_SIZE = "maxFrameSize";
  /**
   * Maximum number of threads to use for processing. Cap on the value of {@link ExecutionContext#threadCount()}.
   * @deprecated Use {@link QueryContextParameters#MAX_THREADS} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_MAX_THREADS = "maxThreads";
  /**
   * Number of segments to load ahead of them being needed. Used when setting up {@link ReadableInputQueue}.
   * <p>
   * A worker may be configured with a local default for this value. When this context value is set, it always wins;
   * the worker-local default applies only when this context value is absent.
   * @deprecated Use {@link QueryContextParameters#SEGMENT_LOAD_AHEAD_COUNT} instead.
   */
  @Deprecated(since = "39.0.0", forRemoval = true)
  public static final String CTX_SEGMENT_LOAD_AHEAD_COUNT = "segmentLoadAheadCount";

  private static final Pattern LOOKS_LIKE_JSON_ARRAY = Pattern.compile("^\\s*\\[.*", Pattern.DOTALL);

  public static String getMSQMode(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.MSQ_MODE);
  }

  public static int getMaxRowsMaterializedInWindow(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.MAX_ROWS_MATERIALIZED_IN_WINDOW);
  }

  public static boolean isWindowFunctionOperatorTransformationEnabled(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.WINDOW_FUNCTION_OPERATOR_TRANSFORMATION);
  }

  public static int getMaxConcurrentStagesWithDefault(
      final QueryContext queryContext,
      final int defaultMaxConcurrentStages
  )
  {
    return queryContext.getOrDefault(
        QueryContextParameters.MAX_CONCURRENT_STAGES,
        defaultMaxConcurrentStages
    );
  }

  public static boolean getVirtualStoragePartialDownloadsEnabled(
      final QueryContext queryContext,
      final boolean defaultValue
  )
  {
    return queryContext.getOrDefault(QueryContextParameters.VIRTUAL_STORAGE_PARTIAL_DOWNLOADS_ENABLED, defaultValue);
  }

  public static boolean isDurableStorageEnabled(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.DURABLE_SHUFFLE_STORAGE);
  }

  public static boolean isFaultToleranceEnabled(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.FAULT_TOLERANCE);
  }

  public static boolean isFailOnEmptyInsertEnabled(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.FAIL_ON_EMPTY_INSERT);
  }

  /**
   * Whether range partitioning that cannot be honored because the data holds multi-valued fields should fail rather
   * than fall back to dynamic partitioning. Ingestion wants the fallback, which stores the rows either way; compaction
   * does not, because the segments it writes would then disagree with the partitionsSpec its config asks for, and it
   * would keep recompacting the interval trying to reach a layout it never produces.
   */
  public static boolean isFailOnMultiValuedRangePartition(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.FAIL_ON_MULTI_VALUED_RANGE_PARTITION);
  }

  public static boolean shouldWaitForSegmentLoad(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.WAIT_UNTIL_SEGMENTS_LOAD);
  }

  public static boolean isReindex(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.IS_REINDEX);
  }

  public static long getMaxInputBytesPerWorker(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.MAX_INPUT_BYTES_PER_WORKER);
  }

  public static int getMaxInputFilesPerWorker(final QueryContext queryContext)
  {
    final int value = queryContext.getOrDefault(QueryContextParameters.MAX_INPUT_FILES_PER_WORKER);
    if (value <= 0) {
      throw InvalidInput.exception(
          "%s must be a positive integer, got[%d]",
          QueryContextParameters.MAX_INPUT_FILES_PER_WORKER,
          value
      );
    }
    return value;
  }

  public static int getMaxPartitions(final QueryContext queryContext)
  {
    final int value = queryContext.getOrDefault(QueryContextParameters.MAX_PARTITIONS);
    if (value <= 0) {
      throw InvalidInput.exception("%s must be a positive integer, got[%d]", QueryContextParameters.MAX_PARTITIONS, value);
    }
    return value;
  }

  public static int getMaxClusteredByColumns(final QueryContext queryContext)
  {
    final int value = queryContext.getOrDefault(
        QueryContextParameters.MAX_CLUSTERED_BY_COLUMNS,
        Limits.MAX_CLUSTERED_BY_COLUMNS
    );
    if (value <= 0) {
      throw InvalidInput.exception(
          "%s must be a positive integer, got[%d]",
          QueryContextParameters.MAX_CLUSTERED_BY_COLUMNS,
          value
      );
    }
    return value;
  }

  public static ClusterStatisticsMergeMode getClusterStatisticsMergeMode(QueryContext queryContext)
  {
    return QueryContexts.getAsEnum(
        QueryContextParameters.CLUSTER_STATISTICS_MERGE_MODE.getName(),
        queryContext.getOrDefault(QueryContextParameters.CLUSTER_STATISTICS_MERGE_MODE),
        ClusterStatisticsMergeMode.class
    );
  }

  public static SketchEncoding getSketchEncoding(QueryContext queryContext)
  {
    return QueryContexts.getAsEnum(
        QueryContextParameters.SKETCH_ENCODING.getName(),
        queryContext.getOrDefault(QueryContextParameters.SKETCH_ENCODING),
        SketchEncoding.class
    );
  }

  public static boolean isFinalizeAggregations(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.FINALIZE_AGGREGATIONS);
  }

  public static SegmentSource getSegmentSources(final QueryContext queryContext, final SegmentSource defaultSource)
  {
    return QueryContexts.getAsEnum(
        QueryContextParameters.INCLUDE_SEGMENT_SOURCE.getName(),
        queryContext.get(QueryContextParameters.INCLUDE_SEGMENT_SOURCE),
        SegmentSource.class,
        defaultSource
    );
  }

  public static WorkerAssignmentStrategy getAssignmentStrategy(final QueryContext queryContext)
  {
    return QueryContexts.getAsEnum(
        QueryContextParameters.TASK_ASSIGNMENT_STRATEGY.getName(),
        queryContext.getOrDefault(QueryContextParameters.TASK_ASSIGNMENT_STRATEGY),
        WorkerAssignmentStrategy.class
    );
  }

  public static int getMaxNumTasks(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.MAX_NUM_TASKS);
  }

  public static int getRowsPerSegment(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.ROWS_PER_SEGMENT);
  }

  public static int getRowsPerPage(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.ROWS_PER_PAGE);
  }

  public static boolean removeNullBytes(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.REMOVE_NULL_BYTES);
  }

  public static boolean isBackgroundFetchExternalFiles(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.BACKGROUND_FETCH_EXTERNAL_FILES);
  }

  public static boolean isUseCombiner(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.USE_COMBINER);
  }

  public static boolean isDartQuery(final QueryContext queryContext)
  {
    return queryContext.has(QueryContextParameters.DART_QUERY_ID);
  }

  public static MSQSelectDestination getSelectDestination(final QueryContext queryContext)
  {
    MSQSelectDestination destination = QueryContexts.getAsEnum(
        QueryContextParameters.SELECT_DESTINATION.getName(),
        queryContext.getOrDefault(QueryContextParameters.SELECT_DESTINATION),
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

  public static int getMaxRowsInMemory(final QueryContext queryContext)
  {
    if (queryContext.has(QueryContextParameters.MAX_ROWS_IN_MEMORY)) {
      return queryContext.getOrDefault(QueryContextParameters.MAX_ROWS_IN_MEMORY);
    }
    return queryContext.getOrDefault(QueryContextParameters.ROWS_IN_MEMORY);
  }

  public static Integer getMaxNumSegments(final QueryContext queryContext)
  {
    // The default is null, if the context is not set.
    return queryContext.get(QueryContextParameters.MAX_NUM_SEGMENTS);
  }

  public static List<String> getSortOrder(final QueryContext queryContext)
  {
    return decodeList(
        QueryContextParameters.SEGMENT_SORT_ORDER,
        queryContext.get(QueryContextParameters.SEGMENT_SORT_ORDER)
    );
  }

  @Nullable
  public static IndexSpec getIndexSpec(final QueryContext queryContext, final ObjectMapper objectMapper)
  {
    return decodeIndexSpec(queryContext.get(QueryContextParameters.INDEX_SPEC), objectMapper);
  }

  public static long getMaxParseExceptions(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(
        QueryContextParameters.MAX_PARSE_EXCEPTIONS,
        MSQWarnings.DEFAULT_MAX_PARSE_EXCEPTIONS_ALLOWED
    );
  }

  public static boolean useAutoColumnSchemas(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.USE_AUTO_COLUMN_SCHEMAS);
  }

  public static ArrayIngestMode getArrayIngestMode(final QueryContext queryContext)
  {
    return QueryContexts.getAsEnum(
        QueryContextParameters.ARRAY_INGEST_MODE.getName(),
        queryContext.getOrDefault(QueryContextParameters.ARRAY_INGEST_MODE),
        ArrayIngestMode.class
    );
  }

  public static int getTargetPartitionsPerWorkerWithDefault(
      final QueryContext queryContext,
      final int defaultValue
  )
  {
    return queryContext.getOrDefault(QueryContextParameters.TARGET_PARTITIONS_PER_WORKER, defaultValue);
  }

  /**
   * Whether new counters (anything other than channel, sortProgress, warnings, segmentGenerationProgress) should
   * be included in reports. This parameter is necessary because prior to Druid 31, we lacked
   * {@link NilQueryCounterSnapshot} as a default counter, which means that {@link SqlStatementResourceHelper} and
   * {@link ControllerResource#httpPostCounters} would throw errors when encountering new counter types that they do
   * not yet recognize. This causes problems during rolling updates.
   *
   * For a rolling update from a version prior to Druid 31, {@link QueryContextParameters#INCLUDE_ALL_COUNTERS} must be
   * set to "false" by the job submitter.
   */
  public static boolean getIncludeAllCounters(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.INCLUDE_ALL_COUNTERS);
  }

  /**
   * Whether workers should send live counter updates to the controller via the message relay. When enabled, workers
   * periodically send counter snapshots to the controller, allowing the controller to have more up-to-date progress
   * information. See {@link QueryContextParameters#LIVE_REPORT_COUNTERS}.
   */
  public static boolean getLiveReportCounters(final QueryContext queryContext, final boolean defaultValue)
  {
    return queryContext.getOrDefault(QueryContextParameters.LIVE_REPORT_COUNTERS, defaultValue);
  }

  public static boolean isForceSegmentSortByTime(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.FORCE_SEGMENT_SORT_BY_TIME);
  }

  /**
   * Returns the {@link FrameType} to use for row-based frames, given by
   * {@link QueryContextParameters#ROW_BASED_FRAME_TYPE}. This context parameter exists to support rolling updates from
   * older Druid versions. The latest type is given by {@link FrameType#latestRowBased()}, which is set in
   * {@link MultiStageQueryContext#withCommonContext} starting in Druid 34. Once all servers are on Druid 34 or newer,
   * the current-latest type {@link FrameType#ROW_BASED_V2} is used. The default is {@link FrameType#ROW_BASED_V1}.
   */
  public static FrameType getRowBasedFrameType(final QueryContext queryContext)
  {
    return FrameType.forVersion(
        queryContext.getOrDefault(QueryContextParameters.ROW_BASED_FRAME_TYPE).byteValue()
    );
  }

  public static DateTime getStartTime(final QueryContext queryContext)
  {
    // Get the start time from the query context set by the broker.
    final String startTime = queryContext.get(QueryContextParameters.START_TIME);
    if (startTime != null) {
      return DateTimes.of(startTime);
    } else {
      // If it is missing, as could be the case for an older version of the broker, use the current time instead.
      final DateTime now = DateTimes.nowUtc();
      log.warn("Query context does not contain start time. Defaulting to the current time[%s] instead.", now);
      return now;
    }
  }

  @Nullable
  public static DateTime getQueryDeadline(final QueryContext queryContext)
  {
    final String queryDeadline = queryContext.get(QueryContextParameters.QUERY_DEADLINE);
    if (queryDeadline != null) {
      return DateTimes.of(queryDeadline);
    } else {
      return null;
    }
  }

  /**
   * Returns a final query context including common context keys shared across all MSQ engines (task, Dart, etc.).
   */
  public static QueryContext withCommonContext(final QueryContext originalContext)
  {
    final Map<String, Object> overrides = new HashMap<>();

    // Add appropriate finalization to native query context.
    if (!originalContext.has(QueryContextParameters.FINALIZE)) {
      QueryContextParameters.FINALIZE.set(overrides, isFinalizeAggregations(originalContext));
    }

    // This flag is to ensure backward compatibility, as brokers are upgraded after indexers/middlemanagers.
    if (!originalContext.has(QueryContextParameters.WINDOW_FUNCTION_OPERATOR_TRANSFORMATION)) {
      QueryContextParameters.WINDOW_FUNCTION_OPERATOR_TRANSFORMATION.set(overrides, true);
    }

    if (!originalContext.has(QueryContextParameters.ROW_BASED_FRAME_TYPE)) {
      // Use the latest row-based frame type. The default is an older type, to ensure compatibility during rolling
      // updates. Since the Broker is updated last, it's safe to set this property on the Broker.
      QueryContextParameters.ROW_BASED_FRAME_TYPE.set(overrides, (int) FrameType.latestRowBased().version());
    }

    // Add start time.
    final DateTime now = DateTimes.nowUtc();
    QueryContextParameters.START_TIME.set(overrides, now.toString());

    // Add query deadline if not already present (and if timeout is set).
    if (!originalContext.has(QueryContextParameters.QUERY_DEADLINE)) {
      final long timeout = originalContext.getTimeout(QueryContexts.NO_TIMEOUT);
      if (timeout != QueryContexts.NO_TIMEOUT) {
        QueryContextParameters.QUERY_DEADLINE.set(overrides, now.plus(timeout).toString());
      }
    }

    return originalContext.override(overrides);
  }

  public static Set<String> getColumnsExcludedFromTypeVerification(final QueryContext queryContext)
  {
    return new HashSet<>(
        decodeList(
            QueryContextParameters.SKIP_TYPE_VERIFICATION,
            queryContext.get(QueryContextParameters.SKIP_TYPE_VERIFICATION)
        )
    );
  }

  public static int getFrameSize(final QueryContext queryContext)
  {
    return queryContext.getOrDefault(QueryContextParameters.MAX_FRAME_SIZE);
  }

  public static Integer getMaxThreads(final QueryContext queryContext)
  {
    return queryContext.get(QueryContextParameters.MAX_THREADS);
  }

  public static Integer getSegmentLoadAheadCount(final QueryContext queryContext)
  {
    return queryContext.get(QueryContextParameters.SEGMENT_LOAD_AHEAD_COUNT);
  }

  /**
   * Decodes a list from either a JSON or CSV string.
   */
  @VisibleForTesting
  static List<String> decodeList(final QueryContextParameter<String> parameter, @Nullable final String listString)
  {
    final String keyName = parameter.getName();
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
   * Decodes {@link QueryContextParameters#INDEX_SPEC} from either a JSON-encoded string, or POJOs.
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
      throw QueryContexts.badValueException(QueryContextParameters.INDEX_SPEC.getName(), "an indexSpec", indexSpecObject);
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
    final boolean useConcurrentLocks = queryContext.getOrDefault(QueryContextParameters.USE_CONCURRENT_LOCKS);
    if (useConcurrentLocks) {
      return isReplaceQuery ? TaskLockType.REPLACE : TaskLockType.APPEND;
    }
    final TaskLockType taskLockType = QueryContexts.getAsEnum(
        QueryContextParameters.TASK_LOCK_TYPE.getName(),
        queryContext.get(QueryContextParameters.TASK_LOCK_TYPE),
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
        + "remove this key for automatic lock type selection", QueryContextParameters.TASK_LOCK_TYPE);

    if (isReplaceQuery && !(taskLockType.equals(TaskLockType.EXCLUSIVE) || taskLockType.equals(TaskLockType.REPLACE))) {
      throw InvalidInput.exception(
          "TaskLock must be of type [%s] or [%s] for a REPLACE query. Found invalid type [%s] set."
          + appendErrorMessage,
          TaskLockType.EXCLUSIVE,
          TaskLockType.REPLACE,
          taskLockType
      );
    }
    if (!isReplaceQuery && !(taskLockType.equals(TaskLockType.SHARED) || taskLockType.equals(TaskLockType.APPEND))) {
      throw InvalidInput.exception(
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
