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
import org.apache.druid.msq.exec.ClusterStatisticsMergeMode;
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
 */
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

  public static final String CTX_ENABLE_DURABLE_SHUFFLE_STORAGE = "durableShuffleStorage";
  public static final String CTX_CLUSTER_STATISTICS_MERGE_MODE = "clusterStatisticsMergeMode";
  public static final String DEFAULT_CLUSTER_STATISTICS_MERGE_MODE = ClusterStatisticsMergeMode.AUTO.toString();
  private static final boolean DEFAULT_ENABLE_DURABLE_SHUFFLE_STORAGE = false;

  public static final String CTX_DESTINATION = "destination";
  private static final String DEFAULT_DESTINATION = null;

  public static final String CTX_ROWS_PER_SEGMENT = "rowsPerSegment";

  public static final String CTX_ROWS_IN_MEMORY = "rowsInMemory";

  /**
   * Controls sort order within segments. Normally, this is the same as the overall order of the query (from the
   * CLUSTERED BY clause) but it can be overridden.
   */
  public static final String CTX_SORT_ORDER = "segmentSortOrder";

  public static final String CTX_INDEX_SPEC = "indexSpec";

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
        CTX_ENABLE_DURABLE_SHUFFLE_STORAGE,
        DEFAULT_ENABLE_DURABLE_SHUFFLE_STORAGE
    );
  }

  public static ClusterStatisticsMergeMode getClusterStatisticsMergeMode(QueryContext queryContext)
  {
    return ClusterStatisticsMergeMode.valueOf(
        String.valueOf(
            queryContext.getString(
                CTX_CLUSTER_STATISTICS_MERGE_MODE,
                DEFAULT_CLUSTER_STATISTICS_MERGE_MODE
            )
        )
    );
  }

  public static boolean isFinalizeAggregations(final QueryContext queryContext)
  {
    return queryContext.getBoolean(
        CTX_FINALIZE_AGGREGATIONS,
        DEFAULT_FINALIZE_AGGREGATIONS
    );
  }

  public static WorkerAssignmentStrategy getAssignmentStrategy(final QueryContext queryContext)
  {
    String assignmentStrategyString = queryContext.getString(
        CTX_TASK_ASSIGNMENT_STRATEGY,
        DEFAULT_TASK_ASSIGNMENT_STRATEGY
    );

    return WorkerAssignmentStrategy.fromString(assignmentStrategyString);
  }

  public static int getMaxNumTasks(final QueryContext queryContext)
  {
    return queryContext.getInt(
        CTX_MAX_NUM_TASKS,
        DEFAULT_MAX_NUM_TASKS
    );
  }

  public static Object getDestination(final QueryContext queryContext)
  {
    return queryContext.get(
        CTX_DESTINATION,
        DEFAULT_DESTINATION
    );
  }

  public static int getRowsPerSegment(final QueryContext queryContext, int defaultRowsPerSegment)
  {
    return queryContext.getInt(
        CTX_ROWS_PER_SEGMENT,
        defaultRowsPerSegment
    );
  }

  public static int getRowsInMemory(final QueryContext queryContext, int defaultRowsInMemory)
  {
    return queryContext.getInt(
        CTX_ROWS_IN_MEMORY,
        defaultRowsInMemory
    );
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
        return new ObjectMapper().readValue(sortOrderString, new TypeReference<List<String>>() {});
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
}
