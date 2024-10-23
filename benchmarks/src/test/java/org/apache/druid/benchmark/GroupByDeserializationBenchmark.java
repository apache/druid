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

package org.apache.druid.benchmark;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.jackson.AggregatorsModule;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryRunnerTestHelper;
import org.apache.druid.query.aggregation.SerializablePairLongString;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryQueryToolChest;
import org.apache.druid.query.groupby.GroupByStatsProvider;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.column.ColumnType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class GroupByDeserializationBenchmark
{

  static {
    NullHandling.initializeForTests();
    BuiltInTypesModule.registerHandlersAndSerde();
    AggregatorsModule.registerComplexMetricsAndSerde();
  }

  @Param({"100", "1000"})
  private int numDimensions;

  @Param({"0", "0.25", "0.5", "0.75", "0.85", "0.95", "0.99", "1.0"})
  private double primitiveToComplexDimensionRatio;

  @Param({"json", "serializablePairLongString"})
  private String complexDimensionType;

  @Param({"true", "false"})
  private boolean backwardCompatibility;

  private GroupByQuery sqlQuery;
  private String serializedRow;
  private GroupByQueryQueryToolChest groupByQueryQueryToolChest;
  private ObjectMapper decoratedMapper;

  @Setup(Level.Trial)
  public void setup() throws JsonProcessingException
  {
    final ObjectMapper undecoratedMapper = TestHelper.makeJsonMapper();
    undecoratedMapper.registerModules(BuiltInTypesModule.getJacksonModulesList());
    undecoratedMapper.registerModule(new AggregatorsModule());
    final Pair<GroupByQuery, String> sqlQueryAndResultRow = sqlQueryAndResultRow(
        numDimensions,
        primitiveToComplexDimensionRatio,
        complexDimensionType,
        undecoratedMapper
    );
    sqlQuery = sqlQueryAndResultRow.lhs;
    serializedRow = sqlQueryAndResultRow.rhs;

    groupByQueryQueryToolChest = new GroupByQueryQueryToolChest(
        null,
        () -> new GroupByQueryConfig()
        {
          @Override
          public boolean isIntermediateResultAsMapCompat()
          {
            return backwardCompatibility;
          }
        },
        null,
        null,
        new GroupByStatsProvider()
    );

    decoratedMapper = groupByQueryQueryToolChest.decorateObjectMapper(undecoratedMapper, sqlQuery);
  }

  @Benchmark
  @BenchmarkMode(Mode.AverageTime)
  @OutputTimeUnit(TimeUnit.MICROSECONDS)
  public void deserializeResultRows(Blackhole blackhole) throws JsonProcessingException
  {
    blackhole.consume(decoratedMapper.readValue(serializedRow, ResultRow.class));
  }

  private static Pair<GroupByQuery, String> sqlQueryAndResultRow(
      final int numDimensions,
      final double primitiveToComplexDimensionRatio,
      final String complexDimensionType,
      final ObjectMapper mapper
  ) throws JsonProcessingException
  {
    final int numPrimitiveDimensions = (int) Math.floor(primitiveToComplexDimensionRatio * numDimensions);
    final int numComplexDimensions = numDimensions - numPrimitiveDimensions;

    final List<DimensionSpec> dimensions = new ArrayList<>();
    final List<Object> rowList = new ArrayList<>();

    // Add timestamp
    rowList.add(DateTimes.of("2000").getMillis());

    for (int i = 0; i < numPrimitiveDimensions; ++i) {
      dimensions.add(
          new DefaultDimensionSpec(
              StringUtils.format("primitive%d", i),
              StringUtils.format("primitive%d", i),
              ColumnType.STRING
          )
      );
      rowList.add("foo");
    }

    for (int i = 0; i < numComplexDimensions; ++i) {
      dimensions.add(
          new DefaultDimensionSpec(
              StringUtils.format("complex%d", i),
              StringUtils.format("complex%d", i),
              ColumnType.ofComplex(complexDimensionType)
          )
      );

      // Serialized version of this object is a valid value for both json and long-string pair dimensions
      rowList.add(new SerializablePairLongString(1L, "test"));
    }

    // Add aggregator
    rowList.add(100);

    // Add post aggregator
    rowList.add(10.0);

    GroupByQuery query = GroupByQuery.builder()
                                     .setDataSource("foo")
                                     .setQuerySegmentSpec(QueryRunnerTestHelper.FULL_ON_INTERVAL_SPEC)
                                     .setDimensions(dimensions)
                                     .setAggregatorSpecs(QueryRunnerTestHelper.ROWS_COUNT)
                                     .setPostAggregatorSpecs(Collections.singletonList(new ConstantPostAggregator(
                                         "post",
                                         10
                                     )))
                                     .setContext(ImmutableMap.of(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, true))
                                     .setGranularity(Granularities.DAY)
                                     .build();

    return Pair.of(query, mapper.writeValueAsString(rowList));
  }
}
