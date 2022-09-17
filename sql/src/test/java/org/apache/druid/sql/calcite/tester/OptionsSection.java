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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * The options test case section.
 */
public class OptionsSection extends TestSection
{
  /**
   * Specifies the "user" (actually, authentication result) to use.
   * The user is a regular user by default. Set to "super" to run as
   * the super user.
   */
  public static final String USER_OPTION = "user";
  public static final String MERGE_BUFFER_COUNT = "mergeBufferCount";

  /**
   * Filter on a test case or run section that says whether the test or
   * run should be done for each of the SQL-compatible null modes.
   * "true" means use SQL-compatible nulls, "false" means use "replace nulls
   * with default" and "both" means the expected results are the same in
   * both cases.
   */
  public static final String SQL_COMPATIBLE_NULLS = "sqlCompatibleNulls";
  public static final String NULL_HANDLING_BOTH = "both";

  /**
   * Indicates that results should be compared as Java objects, with a
   * delta used for float and double values.
   */
  public static final String TYPED_COMPARE = "typedCompare";

  // Planner variations. Corresponds to the various settings
  // in BaseCalciteTest. Since each of those configs alters only
  // one value from the default, these are also the name of the
  // PlannerConfig options which are changed.
  public static final String PLANNER_MAX_TOP_N = "planner.maxTopNLimit";
  public static final String PLANNER_APPROX_COUNT_DISTINCT = "planner.useApproximateCountDistinct";
  public static final String PLANNER_APPROX_TOP_N = "planner.useApproximateTopN";
  public static final String PLANNER_REQUIRE_TIME_CONDITION = "planner.requireTimeCondition";
  public static final String PLANNER_USE_GROUPING_SET_FOR_EXACT_DISTINCT = "planner.useGroupingSetForExactDistinct";
  public static final String PLANNER_COMPUTE_INNER_JOIN_COST_AS_FILTER = "planner.computeInnerJoinCostAsFilter";
  public static final String PLANNER_NATIVE_QUERY_EXPLAIN = "planner.useNativeQueryExplain";
  public static final String PLANNER_MAX_NUMERIC_IN_FILTERS = "planner.maxNumericInFilters";
  public static final String PLANNER_SQL_TIME_ZONE = "planner.sqlTimeZone";

  /**
   * Vectorization option. This option represents a bundle of context
   * options. It is represented as an option to avoid copy/paste of the
   * details. Also, if those details change, only the code that handles this
   * option changes: we don't have to also change all the test cases.
   */
  public static final String VECTORIZE_OPTION = "vectorize";

  public static final String FAILURE_OPTION = "failure";
  public static final String FAIL_AT_RUN = "run";
  public static final String FAIL_AT_PLAN = "plan";

  /**
   * Causes the test code to unescape Java-encoded Unicode characters
   * in the SQL string. Used for one test case:
   * {@code CalciteQueryTset.testUnicodeFilterAndGroupBy} which
   * uses a Hebrew character which is difficult to paste into the
   * test {code .case} file. It is uses a Java-encoded Unicode sequence
   * instead.
   */
  public static final String UNICODE_ESCAPE_OPTION = "unicodeEscapes";

  /**
   * Causes {@code ExpressionProcessingConfig} to allow nested arrays
   * by calling {@code initializeForTests(true)}.
   */
  public static final String ALLOW_NESTED_ARRAYS = "allowNestedArrays";
  public static final String PROVIDER_CLASS = "provider";

  /**
   * Set ExpressionProcessing.initializeForHomogenizeNullMultiValueStrings()
   * Used in only one multi-value string test case.
   */
  public static final String HOMOGENIZE_NULL_MULTI_VALUE_STRINGS = "homogenizeNullMultiValueStrings";

  protected final Map<String, String> options;

  protected OptionsSection(Map<String, String> options)
  {
    this(options, false);
  }

  protected OptionsSection(Map<String, String> options, boolean copy)
  {
    super(Section.OPTIONS.sectionName(), copy);
    this.options = options;
  }

  @Override
  public TestSection.Section section()
  {
    return TestSection.Section.OPTIONS;
  }

  @Override
  public TestSection copy()
  {
    return new OptionsSection(options, true);
  }

  public Map<String, String> options()
  {
    return options;
  }

  public String get(String key)
  {
    return options.get(key);
  }

  public List<String> sorted()
  {
    List<String> keys = new ArrayList<>(options.keySet());
    Collections.sort(keys);
    List<String> sorted = new ArrayList<>();
    for (String key : keys) {
      sorted.add(key + "=" + options.get(key));
    }
    return sorted;
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == this) {
      return true;
    }
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    OptionsSection other = (OptionsSection) o;
    return options.equals(other.options);
  }

  /**
   * Never used (doesn't make sense). But, needed to make static checks happy.
   */
  @Override
  public int hashCode()
  {
    return Objects.hash(options);
  }

  @Override
  public void writeSection(TestCaseWriter writer) throws IOException
  {
    writer.emitOptions(options);
  }
}
