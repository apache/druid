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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerResult;
import org.apache.druid.sql.calcite.rel.DruidRel;
import org.apache.druid.sql.calcite.rel.DruidUnionRel;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

  public static String valueToString(Object value)
  {
    if (value == null) {
      return "\\N";
    } else if (value instanceof String) {
      return "\"" + StringUtils.replace((String) value, "\"", "\\\"") + "\"";
    } else {
      return value.toString();
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
                OptionsSection.PLANNER_MAX_TOP_N,
                options.get(OptionsSection.PLANNER_MAX_TOP_N),
                base.getMaxTopNLimit()))
        .useApproximateCountDistinct(
            booleanOption(
                options,
                OptionsSection.PLANNER_APPROX_COUNT_DISTINCT,
                base.isUseApproximateCountDistinct()))
        .useApproximateTopN(
            booleanOption(
                options,
                OptionsSection.PLANNER_APPROX_TOP_N,
                base.isUseApproximateTopN()))
        .requireTimeCondition(
            booleanOption(
                options,
                OptionsSection.PLANNER_REQUIRE_TIME_CONDITION,
                base.isRequireTimeCondition()))
        .useGroupingSetForExactDistinct(
            booleanOption(
                options,
                OptionsSection.PLANNER_USE_GROUPING_SET_FOR_EXACT_DISTINCT,
                base.isUseGroupingSetForExactDistinct()))
        .computeInnerJoinCostAsFilter(
            booleanOption(
                options,
                OptionsSection.PLANNER_COMPUTE_INNER_JOIN_COST_AS_FILTER,
                base.isComputeInnerJoinCostAsFilter()))
        .useNativeQueryExplain(
            booleanOption(
                options,
                OptionsSection.PLANNER_NATIVE_QUERY_EXPLAIN,
                base.isUseNativeQueryExplain()))
        .maxNumericInFilters(
            QueryContexts.getAsInt(
                OptionsSection.PLANNER_MAX_NUMERIC_IN_FILTERS,
                options.get(OptionsSection.PLANNER_MAX_NUMERIC_IN_FILTERS),
                base.getMaxNumericInFilters()));

    String timeZone = options.get(OptionsSection.PLANNER_SQL_TIME_ZONE);
    if (timeZone != null) {
      builder.sqlTimeZone(DateTimes.inferTzFromString(timeZone));
    }
    return builder.build();
  }
}
