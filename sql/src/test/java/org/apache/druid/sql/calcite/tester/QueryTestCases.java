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

package org.apache.druid.sql.calcite.tester;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.Numbers;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryContexts.Vectorize;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnionRel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Collection of utilities for working with test cases.
 */
public class QueryTestCases
{
  public static String serializeQuery(ObjectMapper mapper, Object query)
  {
    return formatJson(mapper, query);
  }

  public static String serializeDruidRel(ObjectMapper mapper, DruidRel<?> druidRel)
  {
    // Note: must pass false to toDruidQuery (that is, don't finalize
    // aggregations) to match the native queries expected by the
    // various CalciteXQueryTest classes.
    return serializeQuery(mapper, createQuery(druidRel));
  }

  /**
   * Creates a native query to serialize. The union query is not a
   * native query: it is instead handled as a list of such queries.
   * We simulate that here by creating an "artificial" union query.
   */
  public static Object createQuery(DruidRel<?> druidRel)
  {
    if (druidRel instanceof DruidUnionRel) {
      List<Object> inputs = new ArrayList<>();
      for (RelNode input : druidRel.getInputs()) {
        inputs.add(createQuery((DruidRel<?>) input));
      }
      return ImmutableMap.of(
          "artificialQueryType",
          "union",
          "inputs",
          inputs);
    } else {
      return druidRel.toDruidQuery(false).getQuery();
    }
  }

  /**
   * Reformat the plan. It includes a big wad of JSON all on one line
   * which is hard to read. This reformats into a mixture of formatted
   * JSON and the Calcite formatting. Ugly code, but the result is less
   * ugly than the single long line.
   */
  public static String formatExplain(ObjectMapper mapper, String plan, String signature)
  {
    StringBuilder buf = new StringBuilder();
    Pattern p = Pattern.compile("DruidQueryRel\\(query=\\[(.*)], signature=\\[(.*)]\\)");
    Matcher m = p.matcher(plan.trim());
    if (m.matches()) {
      buf.append("DruidQueryRel(query=[(\n")
         .append(reformatJson(mapper, m.group(1)))
         .append(",\nsignature=[(\n  ")
         // The signature only looks like JSON: it does not have proper quoting.
         .append(m.group(2))
         .append("\n])\n");
    } else {
      buf.append(plan.trim()).append("\n");
    }
    // Separate the signature from the above part.
    buf.append("---\n")
       .append(reformatJson(mapper, signature))
       .append("\n");
    return buf.toString();
  }

  public static String[] formatSchema(PlannerResult plannerResult)
  {
    List<RelDataTypeField> fields = plannerResult.rowType().getFieldList();
    String[] actual = new String[fields.size()];
    for (int i = 0; i < actual.length; i++) {
      RelDataTypeField field = fields.get(i);
      actual[i] = field.getName() + " " + field.getType();
    }
    return actual;
  }

  public static String formatJson(ObjectMapper mapper, Object obj)
  {
    try {
      return mapper
          .writerWithDefaultPrettyPrinter()
          .writeValueAsString(obj);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException("JSON conversion failed", e);
    }
  }

  public static String reformatJson(ObjectMapper mapper, String json)
  {
    try {
      Object obj = mapper.readValue(json, Object.class);
      return formatJson(mapper, obj);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException("JSON parse failed", e);
    }
  }

  public static List<String> resultsToJson(List<Object[]> results, ObjectMapper mapper)
  {
    try {
      List<String> jsonLines = new ArrayList<>();
      for (Object[] row : results) {
        jsonLines.add(mapper.writeValueAsString(row));
      }
      return jsonLines;
    }
    catch (Exception e) {
      throw new IAE(e, "Results conversion to JSON failed");
    }
  }

  public static Map<String, Object> rewriteContext(Map<String, Object> context)
  {
    Map<String, Object> copy = new HashMap<>(context);
    copy.remove(QueryContexts.DEFAULT_TIMEOUT_KEY);
    copy.remove(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY);
    copy.remove("sqlCurrentTimestamp");
    copy.remove("sqlQueryId");
    copy.remove("vectorize");
    copy.remove("vectorizeVirtualColumns");
    copy.remove("vectorSize");
    return copy;
  }

  public static String unquote(String value)
  {
    if (value.length() < 2) {
      return value;
    }
    char first = value.charAt(0);
    if (first != '\'' && first != '"') {
      return value;
    }
    char last = value.charAt(value.length() - 1);
    if (last != first) {
      return value;
    }
    return value.substring(1, value.length() - 1);
  }

  public static boolean booleanOption(Map<String, ?> options, String key, boolean defaultValue)
  {
    return QueryContexts.getAsBoolean(key, options.get(key), defaultValue);
  }

  public static PlannerConfig applyOptions(PlannerConfig base, Map<String, String> options)
  {
    PlannerConfig.Builder builder = base
        .toBuilder()
        .maxTopNLimit(
            QueryContexts.getAsInt(
                TestOptions.PLANNER_MAX_TOP_N,
                options.get(TestOptions.PLANNER_MAX_TOP_N),
                base.getMaxTopNLimit()))
        .useApproximateCountDistinct(
            booleanOption(
                options,
                TestOptions.PLANNER_APPROX_COUNT_DISTINCT,
                base.isUseApproximateCountDistinct()))
        .useApproximateTopN(
            booleanOption(
                options,
                TestOptions.PLANNER_APPROX_TOP_N,
                base.isUseApproximateTopN()))
        .requireTimeCondition(
            booleanOption(
                options,
                TestOptions.PLANNER_REQUIRE_TIME_CONDITION,
                base.isRequireTimeCondition()))
        .useGroupingSetForExactDistinct(
            booleanOption(
                options,
                TestOptions.PLANNER_USE_GROUPING_SET_FOR_EXACT_DISTINCT,
                base.isUseGroupingSetForExactDistinct()))
        .computeInnerJoinCostAsFilter(
            booleanOption(
                options,
                TestOptions.PLANNER_COMPUTE_INNER_JOIN_COST_AS_FILTER,
                base.isComputeInnerJoinCostAsFilter()))
        .useNativeQueryExplain(
            booleanOption(
                options,
                TestOptions.PLANNER_NATIVE_QUERY_EXPLAIN,
                base.isUseNativeQueryExplain()))
        .maxNumericInFilters(
            QueryContexts.getAsInt(
                TestOptions.PLANNER_MAX_NUMERIC_IN_FILTERS,
                options.get(TestOptions.PLANNER_MAX_NUMERIC_IN_FILTERS),
                base.getMaxNumericInFilters()));

    String timeZone = options.get(TestOptions.PLANNER_SQL_TIME_ZONE);
    if (timeZone != null) {
      builder.sqlTimeZone(DateTimes.inferTzFromString(timeZone));
    }
    return builder.build();
  }
  public enum EntryType
  {
    STRING,
    BOOLEAN,
    INT,
    LONG,
    VECTORIZE,
    OBJECT;

    public Object parse(String value)
    {
      if (value == null) {
        return null;
      }
      if (this != STRING) {
        value = value.trim();
        if (value.length() == 0) {
          return null;
        }
      }
      switch (this) {
        case BOOLEAN:
          return Numbers.parseBoolean(value);
        case LONG:
          return Numbers.parseLong(value);
        case INT:
          return Numbers.parseInt(value);
        case VECTORIZE:
          return Vectorize.valueOf(StringUtils.toUpperCase(value));
        default:
          return value;
      }
    }
  }

  /**
   * Definition of non-String context variables. At present, provides only the
   * type. This can be expanded to provide other attributes when useful: whether
   * the item is internal or external, whether it is only for the SQL planner, and
   * can be stripped out of the query before execution, the default value, etc.
   */
  public static final ConcurrentHashMap<String, EntryType> ENTRY_DEFNS = new ConcurrentHashMap<>();

  // List of known context keys with type and default value (where known).
  // Some of these are probably internal: add the flag where that is true.

  static {
    ENTRY_DEFNS.put(QueryContexts.BROKER_PARALLEL_MERGE_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.BROKER_PARALLEL_MERGE_INITIAL_YIELD_ROWS_KEY, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.BROKER_PARALLEL_MERGE_SMALL_BATCH_ROWS_KEY, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.BROKER_PARALLELISM, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.BY_SEGMENT_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.DEFAULT_TIMEOUT_KEY, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.ENABLE_DEBUG, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.FINALIZE_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.IN_SUB_QUERY_THRESHOLD_KEY, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.JOIN_FILTER_PUSH_DOWN_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.JOIN_FILTER_REWRITE_ENABLE_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.JOIN_FILTER_REWRITE_MAX_SIZE_KEY, EntryType.LONG);
    ENTRY_DEFNS.put(
        QueryContexts.JOIN_FILTER_REWRITE_VALUE_COLUMN_FILTERS_ENABLE_KEY,
          EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.MAX_NUMERIC_IN_FILTERS, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.MAX_QUEUED_BYTES_KEY, EntryType.LONG);
    ENTRY_DEFNS.put(QueryContexts.MAX_SCATTER_GATHER_BYTES_KEY, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.MAX_SUBQUERY_ROWS_KEY, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.NUM_RETRIES_ON_MISSING_SEGMENTS_KEY, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.POPULATE_CACHE_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.POPULATE_RESULT_LEVEL_CACHE_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.PRIORITY_KEY, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.RETURN_PARTIAL_RESULTS_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.REWRITE_JOIN_TO_FILTER_ENABLE_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.SECONDARY_PARTITION_PRUNING_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.SERIALIZE_DATE_TIME_AS_LONG_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.SERIALIZE_DATE_TIME_AS_LONG_INNER_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.SQL_JOIN_LEFT_SCAN_DIRECT, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.TIME_BOUNDARY_PLANNING_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.TIMEOUT_KEY, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.UNCOVERED_INTERVALS_LIMIT_KEY, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.USE_CACHE_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.USE_FILTER_CNF_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.USE_RESULT_LEVEL_CACHE_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.VECTOR_SIZE_KEY, EntryType.INT);
    ENTRY_DEFNS.put(QueryContexts.VECTORIZE_KEY, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(QueryContexts.VECTORIZE_VIRTUAL_COLUMNS_KEY, EntryType.BOOLEAN);

    ENTRY_DEFNS.put(GroupByQueryConfig.CTX_KEY_FORCE_LIMIT_PUSH_DOWN, EntryType.BOOLEAN);
    ENTRY_DEFNS.put(GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, EntryType.BOOLEAN);

    // From PlannerContext: constants not visible here.
    ENTRY_DEFNS.put("sqlOuterLimit", EntryType.INT);
    ENTRY_DEFNS.put("sqlStringifyArrays", EntryType.BOOLEAN);
    ENTRY_DEFNS.put("useApproximateTopN", EntryType.BOOLEAN);

    // From TimeseriesQuery
    ENTRY_DEFNS.put(TimeseriesQuery.SKIP_EMPTY_BUCKETS, EntryType.BOOLEAN);
  }

  /**
   * Get the definition (currently, only the type) of the context key.
   * Defaults to STRING unless a different type is explicitly registered.
   */
  public static EntryType definition(String key)
  {
    EntryType defn = ENTRY_DEFNS.get(key);
    return defn == null ? EntryType.STRING : defn;
  }
}
