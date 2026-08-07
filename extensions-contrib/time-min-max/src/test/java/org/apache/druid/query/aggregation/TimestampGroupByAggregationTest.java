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

package org.apache.druid.query.aggregation;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.easymock.EasyMock;
import org.joda.time.DateTime;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.ZipFile;

public class TimestampGroupByAggregationTest
{
  private AggregationTestHelper helper;

  @TempDir
  public File temporaryFolder;

  private ColumnSelectorFactory selectorFactory;
  private TestObjectColumnSelector selector;

  private Timestamp[] values = new Timestamp[10];

  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    final List<List<Object>> partialConstructors = ImmutableList.of(
        ImmutableList.of("timeMin", "tmin", "time_min", DateTimes.of("2011-01-12T01:00:00.000Z")),
        ImmutableList.of("timeMax", "tmax", "time_max", DateTimes.of("2011-01-31T01:00:00.000Z"))
    );

    for (final List<Object> partialConstructor : partialConstructors) {
      for (GroupByQueryConfig config : testConfigs()) {
        final List<Object> constructor = Lists.newArrayList(partialConstructor);
        constructor.add(config);
        constructors.add(constructor.toArray());
      }
    }

    return constructors;
  }

  private static List<GroupByQueryConfig> testConfigs()
  {
    return List.of(
        new GroupByQueryConfig()
        {
          @Override
          public int getBufferGrouperInitialBuckets()
          {
            return 4;
          }
        },
        new GroupByQueryConfig()
        {
          @Override
          public int getBufferGrouperMaxSize()
          {
            return 2;
          }

          @Override
          public HumanReadableBytes getMaxOnDiskStorage()
          {
            return HumanReadableBytes.valueOf(10L * 1024 * 1024);
          }
        },
        new org.apache.druid.jackson.DefaultObjectMapper().convertValue(
            java.util.Map.of(
                "maxSelectorDictionarySize", 20,
                "maxMergingDictionarySize", 400,
                "maxOnDiskStorage", 10L * 1024 * 1024
            ),
            GroupByQueryConfig.class
        ),
        new GroupByQueryConfig()
        {
          @Override
          public int getNumParallelCombineThreads()
          {
            return 2;
          }
        }
    );
  }

  private String aggType;
  private String aggField;
  private String groupByField;
  private DateTime expected;
  private GroupByQueryConfig config;

  public void initTimestampGroupByAggregationTest(
      String aggType,
      String aggField,
      String groupByField,
      DateTime expected,
      GroupByQueryConfig config
  )
  {
    this.aggType = aggType;
    this.aggField = aggField;
    this.groupByField = groupByField;
    this.expected = expected;
    this.config = config;
  }

  private void setup()
  {
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelperWithTempDir(
        new TimestampMinMaxModule().getJacksonModules(),
        config,
        temporaryFolder
    );

    selector = new TestObjectColumnSelector<>(values);
    selectorFactory = EasyMock.createMock(ColumnSelectorFactory.class);
    EasyMock.expect(selectorFactory.makeColumnValueSelector("test")).andReturn(selector);
    EasyMock.replay(selectorFactory);
  }

  @AfterEach
  public void teardown() throws IOException
  {
    helper.close();
  }

  private AggregatorFactory makeTimestampAggregator(String name, String fieldName)
  {
    return "timeMin".equals(aggType)
        ? new TimestampMinAggregatorFactory(name, fieldName, null)
        : new TimestampMaxAggregatorFactory(name, fieldName, null);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{index}: Test for {0}, config = {1}")
  public void testSimpleDataIngestionAndGroupByTest(
      String aggType,
      String aggField,
      String groupByField,
      DateTime expected,
      GroupByQueryConfig config
  ) throws Exception
  {
    initTimestampGroupByAggregationTest(aggType, aggField, groupByField, expected, config);
    setup();
    List<AggregatorFactory> aggregators = List.of(makeTimestampAggregator(aggField, "timestamp"));

    GroupByQuery groupByQuery = GroupByQuery.builder()
                                            .setDataSource("test_datasource")
                                            .setGranularity(Granularities.MONTH)
                                            .setDimensions(new DefaultDimensionSpec("product", "product"))
                                            .setAggregatorSpecs(makeTimestampAggregator(groupByField, aggField))
                                            .setInterval("2011-01-01T00:00:00.000Z/2011-05-01T00:00:00.000Z")
                                            .build();

    final Sequence<ResultRow> seq;
    try (final ZipFile zip = new ZipFile(
        new File(this.getClass().getClassLoader().getResource("druid.sample.tsv.zip").toURI())
    );
         final InputStream inputStream = zip.getInputStream(zip.getEntry("druid.sample.tsv"))) {
      seq = helper.createIndexAndRunQueryOnSegment(
          inputStream,
          new InputRowSchema(
              new TimestampSpec("timestamp", "auto", null),
              new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
              ColumnsFilter.all()
          ),
          DelimitedInputFormat.forColumns(
              List.of("timestamp", "cat", "product", "prefer", "prefer2", "pty_country")
          ),
          aggregators,
          0,
          Granularities.MONTH,
          100,
          groupByQuery
      );
    }

    int groupByFieldNumber = groupByQuery.getResultRowSignature().indexOf(groupByField);

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(36, results.size());
    Assertions.assertEquals(expected, results.get(0).get(groupByFieldNumber));
  }
}
