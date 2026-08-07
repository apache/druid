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

package org.apache.druid.query.aggregation.momentsketch.aggregator;


import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.DoubleDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.HumanReadableBytes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.momentsketch.MomentSketchModule;
import org.apache.druid.query.aggregation.momentsketch.MomentSketchWrapper;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class MomentsSketchAggregatorTest extends InitializedNullHandlingTest
{
  private AggregationTestHelper helper;

  @TempDir
  public File tempFolder;

  public void initMomentsSketchAggregatorTest(final GroupByQueryConfig config)
  {
    MomentSketchModule.registerSerde();
    DruidModule module = new MomentSketchModule();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelperWithTempDir(
        module.getJacksonModules(), config, tempFolder);
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

  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void buildingSketchesAtIngestionTime(final GroupByQueryConfig config) throws Exception
  {
    initMomentsSketchAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            DimensionsSpec.builder()
                          .setDefaultSchemaDimensions(List.of("product"))
                          .setDimensionExclusions(List.of("sequenceNumber"))
                          .build(),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "sequenceNumber", "product", "value", "valueWithNulls")
        ),
        List.of(
            new MomentSketchAggregatorFactory("sketch", "value", 10, true),
            new MomentSketchAggregatorFactory("sketchWithNulls", "valueWithNulls", 10, true)
        ),
        0,
        // minTimestamp
        Granularities.NONE,
        10,
        // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setDimensions(Collections.emptyList())
                    .setAggregatorSpecs(
                        new MomentSketchMergeAggregatorFactory("sketch", 10, true),
                        new MomentSketchMergeAggregatorFactory("sketchWithNulls", 10, true)
                    )
                    .setPostAggregatorSpecs(
                        new MomentSketchQuantilePostAggregator(
                            "quantiles",
                            new FieldAccessPostAggregator("sketch", "sketch"),
                            new double[]{0, 0.5, 1}
                        ),
                        new MomentSketchMinPostAggregator(
                            "min",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new MomentSketchMaxPostAggregator(
                            "max",
                            new FieldAccessPostAggregator("sketch", "sketch")
                        ),
                        new MomentSketchQuantilePostAggregator(
                            "quantilesWithNulls",
                            new FieldAccessPostAggregator("sketchWithNulls", "sketchWithNulls"),
                            new double[]{0, 0.5, 1}
                        ),
                        new MomentSketchMinPostAggregator(
                            "minWithNulls",
                            new FieldAccessPostAggregator("sketchWithNulls", "sketchWithNulls")
                        ),
                        new MomentSketchMaxPostAggregator(
                            "maxWithNulls",
                            new FieldAccessPostAggregator("sketchWithNulls", "sketchWithNulls")
                        )
                    )
                    .setInterval(Intervals.of("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z"))
                    .build()
    );
    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    MomentSketchWrapper sketchObject = (MomentSketchWrapper) row.get(0); // "sketch"
    // 400 total products since this is pre-rollup
    Assertions.assertEquals(400.0, sketchObject.getPowerSums()[0], 1e-10);

    MomentSketchWrapper sketchObjectWithNulls = (MomentSketchWrapper) row.get(1); // "sketchWithNulls"
    // 23 null values (377 when nulls are not replaced with default)
    Assertions.assertEquals(
        377.0,
        sketchObjectWithNulls.getPowerSums()[0],
        1e-10
    );

    double[] quantilesArray = (double[]) row.get(2); // "quantiles"
    Assertions.assertEquals(0, quantilesArray[0], 0.05);
    Assertions.assertEquals(.5, quantilesArray[1], 0.05);
    Assertions.assertEquals(1.0, quantilesArray[2], 0.05);

    Double minValue = (Double) row.get(3); // "min"
    Assertions.assertEquals(0.0011, minValue, 0.0001);

    Double maxValue = (Double) row.get(4); // "max"
    Assertions.assertEquals(0.9969, maxValue, 0.0001);

    double[] quantilesArrayWithNulls = (double[]) row.get(5); // "quantilesWithNulls"
    Assertions.assertEquals(5.0, quantilesArrayWithNulls[0], 0.05);
    Assertions.assertEquals(
        7.57,
        quantilesArrayWithNulls[1],
        0.05
    );
    Assertions.assertEquals(10.0, quantilesArrayWithNulls[2], 0.05);

    Double minValueWithNulls = (Double) row.get(6); // "minWithNulls"
    Assertions.assertEquals(5.0164, minValueWithNulls, 0.0001);

    Double maxValueWithNulls = (Double) row.get(7); // "maxWithNulls"
    Assertions.assertEquals(9.9788, maxValueWithNulls, 0.0001);

  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void buildingSketchesAtQueryTime(final GroupByQueryConfig config) throws Exception
  {
    initMomentsSketchAggregatorTest(config);
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("doubles_build_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            DimensionsSpec.builder()
                          .setDimensions(
                              List.of(
                                  new StringDimensionSchema("product"),
                                  new DoubleDimensionSchema("valueWithNulls")
                              )
                          )
                          .setDimensionExclusions(List.of("sequenceNumber"))
                          .build(),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(
            List.of("timestamp", "sequenceNumber", "product", "value", "valueWithNulls")
        ),
        List.of(new DoubleSumAggregatorFactory("value", "value")),
        0, // minTimestamp
        Granularities.NONE,
        10, // maxRowCount
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setDimensions(Collections.emptyList())
                    .setAggregatorSpecs(
                        new MomentSketchAggregatorFactory("sketch", "value", 10, null),
                        new MomentSketchAggregatorFactory("sketchWithNulls", "valueWithNulls", 10, null)
                    )
                    .setInterval(Intervals.of("2016-01-01T00:00:00.000Z/2016-01-31T00:00:00.000Z"))
                    .build()
    );

    List<ResultRow> results = seq.toList();
    Assertions.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    MomentSketchWrapper sketchObject = (MomentSketchWrapper) row.get(0); // "sketch"
    // 385 total products since roll-up limited by valueWithNulls column
    Assertions.assertEquals(385.0, sketchObject.getPowerSums()[0], 1e-10);

    MomentSketchWrapper sketchObjectWithNulls = (MomentSketchWrapper) row.get(1); // "sketchWithNulls"

    Assertions.assertEquals(377.0, sketchObjectWithNulls.getPowerSums()[0], 1e-10);
  }
}
