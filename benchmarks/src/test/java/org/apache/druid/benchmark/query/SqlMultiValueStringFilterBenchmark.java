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

package org.apache.druid.benchmark.query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.lookup.ImmutableLookupMap;
import org.apache.druid.query.lookup.LookupExtractor;
import org.apache.druid.sql.calcite.planner.PlannerConfig;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Benchmark for the multi-value string filtering functions MV_FILTER_ONLY, MV_FILTER_NONE, MV_FILTER_REGEX, and
 * MV_FILTER_PREFIX applied to expressions (LOOKUP, SUBSTRING, REGEXP_EXTRACT) of multi-value string columns, comparing
 * the plain expression plan with the specialized filtering virtual column plan (with an expression virtual column
 * delegate), and with the extractionFn plan used when {@link PlannerContext#CTX_SQL_USE_EXTRACTION_FNS} is set.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class SqlMultiValueStringFilterBenchmark extends SqlBaseQueryBenchmark
{
  private static final String ENUM_LOOKUP = "enum-lookup";
  private static final String NUMBER_LOOKUP = "number-lookup";
  private static final int NUMBER_LOOKUP_KEYS = 1_000_000;
  private static final int NUMBER_LOOKUP_VALUES = 1_000;

  private static final List<Pair<String, String>> QUERIES = ImmutableList.of(
      // 0: low cardinality multi-value string column (5 values)
      Pair.of(
          SqlBenchmarkDatasets.BASIC,
          "SELECT MV_FILTER_ONLY(LOOKUP(dimMultivalEnumerated, 'enum-lookup'), ARRAY['greeting']), COUNT(*) "
          + "FROM basic GROUP BY 1"
      ),
      // 1:
      Pair.of(
          SqlBenchmarkDatasets.BASIC,
          "SELECT MV_FILTER_NONE(LOOKUP(dimMultivalEnumerated, 'enum-lookup'), ARRAY['greeting']), COUNT(*) "
          + "FROM basic GROUP BY 1"
      ),
      // 2:
      Pair.of(
          SqlBenchmarkDatasets.BASIC,
          "SELECT MV_FILTER_REGEX(LOOKUP(dimMultivalEnumerated, 'enum-lookup'), '^g.*'), COUNT(*) "
          + "FROM basic GROUP BY 1"
      ),
      // 3:
      Pair.of(
          SqlBenchmarkDatasets.BASIC,
          "SELECT MV_FILTER_PREFIX(LOOKUP(dimMultivalEnumerated, 'enum-lookup'), 'g'), COUNT(*) "
          + "FROM basic GROUP BY 1"
      ),
      // 4:
      Pair.of(
          SqlBenchmarkDatasets.BASIC,
          "SELECT COUNT(*) FROM basic "
          + "WHERE MV_FILTER_ONLY(LOOKUP(dimMultivalEnumerated, 'enum-lookup'), ARRAY['greeting']) = 'greeting'"
      ),
      // 5: high cardinality multi-value string columns (1M values)
      Pair.of(
          SqlBenchmarkDatasets.GROUPER,
          "SELECT MV_FILTER_ONLY(LOOKUP(\"multi-string-Uniform-1_000_000\", 'number-lookup'), ARRAY['bucket-1', 'bucket-2']), COUNT(*) "
          + "FROM grouper GROUP BY 1"
      ),
      // 6:
      Pair.of(
          SqlBenchmarkDatasets.GROUPER,
          "SELECT MV_FILTER_ONLY(LOOKUP(\"multi-string-ZipF-1_000_000\", 'number-lookup'), ARRAY['bucket-1', 'bucket-2']), COUNT(*) "
          + "FROM grouper GROUP BY 1"
      ),
      // 7:
      Pair.of(
          SqlBenchmarkDatasets.GROUPER,
          "SELECT MV_FILTER_PREFIX(LOOKUP(\"multi-string-Uniform-1_000_000\", 'number-lookup'), 'bucket-1'), COUNT(*) "
          + "FROM grouper GROUP BY 1"
      ),
      // 8:
      Pair.of(
          SqlBenchmarkDatasets.GROUPER,
          "SELECT COUNT(*) FROM grouper "
          + "WHERE MV_FILTER_ONLY(LOOKUP(\"multi-string-Uniform-1_000_000\", 'number-lookup'), ARRAY['bucket-1']) = 'bucket-1'"
      ),
      // 9: other functions which plan to an extractionFn when sqlUseExtractionFns is set
      Pair.of(
          SqlBenchmarkDatasets.BASIC,
          "SELECT MV_FILTER_ONLY(SUBSTRING(dimMultivalEnumerated, 1, 1), ARRAY['B']), COUNT(*) "
          + "FROM basic GROUP BY 1"
      ),
      // 10:
      Pair.of(
          SqlBenchmarkDatasets.BASIC,
          "SELECT MV_FILTER_ONLY(REGEXP_EXTRACT(dimMultivalEnumerated, '^[A-Z][a-z]'), ARRAY['Ba']), COUNT(*) "
          + "FROM basic GROUP BY 1"
      ),
      // 11:
      Pair.of(
          SqlBenchmarkDatasets.GROUPER,
          "SELECT MV_FILTER_ONLY(SUBSTRING(\"multi-string-Uniform-1_000_000\", 1, 2), ARRAY['12', '34']), COUNT(*) "
          + "FROM grouper GROUP BY 1"
      ),
      // 12:
      Pair.of(
          SqlBenchmarkDatasets.GROUPER,
          "SELECT MV_FILTER_ONLY(REGEXP_EXTRACT(\"multi-string-Uniform-1_000_000\", '[0-9]{2}$'), ARRAY['12', '34']), COUNT(*) "
          + "FROM grouper GROUP BY 1"
      )
  );

  @Param({
      "0",
      "1",
      "2",
      "3",
      "4",
      "5",
      "6",
      "7",
      "8",
      "9",
      "10",
      "11",
      "12"
  })
  private String query;

  /**
   * expression: plain expression virtual columns, forced with {@link PlannerConfig#CTX_KEY_FORCE_EXPRESSION_VIRTUAL_COLUMNS}
   * specialized: default planning, specialized filtering virtual columns with an expression virtual column delegate
   * extractionFn: specialized filtering virtual columns with an extractionFn delegate dimension spec
   */
  @Param({
      "expression",
      "specialized",
      "extractionFn"
  })
  private String plan;

  @Override
  public String getQuery()
  {
    return QUERIES.get(Integer.parseInt(query)).rhs;
  }

  @Override
  public List<String> getDatasources()
  {
    return Collections.singletonList(QUERIES.get(Integer.parseInt(query)).lhs);
  }

  @Override
  protected Map<String, Object> getContext()
  {
    final ImmutableMap.Builder<String, Object> builder =
        ImmutableMap.<String, Object>builder().putAll(super.getContext());
    switch (plan) {
      case "expression":
        builder.put(PlannerConfig.CTX_KEY_FORCE_EXPRESSION_VIRTUAL_COLUMNS, true);
        break;
      case "extractionFn":
        builder.put(PlannerContext.CTX_SQL_USE_EXTRACTION_FNS, true);
        break;
      default:
        break;
    }
    return builder.build();
  }

  @Override
  protected Map<String, LookupExtractor> getLookups()
  {
    // many-to-one lookups, so filtered values include duplicates
    final Map<String, String> enumLookup = ImmutableMap.of(
        "Hello", "greeting",
        "World", "greeting",
        "Foo", "placeholder",
        "Bar", "placeholder",
        "Baz", "placeholder"
    );
    final Map<String, String> numberLookup = Maps.newHashMapWithExpectedSize(NUMBER_LOOKUP_KEYS + 1);
    for (int i = 0; i <= NUMBER_LOOKUP_KEYS; i++) {
      numberLookup.put(String.valueOf(i), "bucket-" + (i % NUMBER_LOOKUP_VALUES));
    }
    return ImmutableMap.of(
        ENUM_LOOKUP,
        ImmutableLookupMap.fromMap(enumLookup).asLookupExtractor(false, () -> StringUtils.toUtf8(ENUM_LOOKUP)),
        NUMBER_LOOKUP,
        ImmutableLookupMap.fromMap(numberLookup).asLookupExtractor(false, () -> StringUtils.toUtf8(NUMBER_LOOKUP))
    );
  }

  @Override
  protected void checkIncompatibleParameters()
  {
    // none of the plans can be vectorized
    if (QueryContexts.Vectorize.FORCE.equals(vectorizeContext)) {
      System.exit(0);
    }
    // auto schema ingests multi-value strings as arrays, but this benchmark is about multi-value strings
    if ("auto".equals(schemaType)) {
      System.exit(0);
    }
    super.checkIncompatibleParameters();
  }
}
