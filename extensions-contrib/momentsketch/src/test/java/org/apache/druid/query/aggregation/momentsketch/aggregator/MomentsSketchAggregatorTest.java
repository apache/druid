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
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

@RunWith(Parameterized.class)
public class MomentsSketchAggregatorTest extends InitializedNullHandlingTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public MomentsSketchAggregatorTest(final GroupByQueryConfig config)
  {
    MomentSketchModule.registerSerde();
    DruidModule module = new MomentSketchModule();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(), config, tempFolder);
  }

  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @Test
  public void buildingSketchesAtIngestionTime() throws Exception
  {
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
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    MomentSketchWrapper sketchObject = (MomentSketchWrapper) row.get(0); // "sketch"
    // 400 total products since this is pre-rollup
    Assert.assertEquals(400.0, sketchObject.getPowerSums()[0], 1e-10);

    MomentSketchWrapper sketchObjectWithNulls = (MomentSketchWrapper) row.get(1); // "sketchWithNulls"
    // 23 null values (377 when nulls are not replaced with default)
    Assert.assertEquals(
        377.0,
        sketchObjectWithNulls.getPowerSums()[0],
        1e-10
    );

    double[] quantilesArray = (double[]) row.get(2); // "quantiles"
    Assert.assertEquals(0, quantilesArray[0], 0.05);
    Assert.assertEquals(.5, quantilesArray[1], 0.05);
    Assert.assertEquals(1.0, quantilesArray[2], 0.05);

    Double minValue = (Double) row.get(3); // "min"
    Assert.assertEquals(0.0011, minValue, 0.0001);

    Double maxValue = (Double) row.get(4); // "max"
    Assert.assertEquals(0.9969, maxValue, 0.0001);

    double[] quantilesArrayWithNulls = (double[]) row.get(5); // "quantilesWithNulls"
    Assert.assertEquals(5.0, quantilesArrayWithNulls[0], 0.05);
    Assert.assertEquals(
        7.57,
        quantilesArrayWithNulls[1],
        0.05
    );
    Assert.assertEquals(10.0, quantilesArrayWithNulls[2], 0.05);

    Double minValueWithNulls = (Double) row.get(6); // "minWithNulls"
    Assert.assertEquals(5.0164, minValueWithNulls, 0.0001);

    Double maxValueWithNulls = (Double) row.get(7); // "maxWithNulls"
    Assert.assertEquals(9.9788, maxValueWithNulls, 0.0001);

  }

  @Test
  public void buildingSketchesAtQueryTime() throws Exception
  {
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
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);

    MomentSketchWrapper sketchObject = (MomentSketchWrapper) row.get(0); // "sketch"
    // 385 total products since roll-up limited by valueWithNulls column
    Assert.assertEquals(385.0, sketchObject.getPowerSums()[0], 1e-10);

    MomentSketchWrapper sketchObjectWithNulls = (MomentSketchWrapper) row.get(1); // "sketchWithNulls"

    Assert.assertEquals(377.0, sketchObjectWithNulls.getPowerSums()[0], 1e-10);
  }
}
