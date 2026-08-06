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

package org.apache.druid.query.aggregation.datasketches.theta.oldapi;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.druid.data.input.ColumnsFilter;
import org.apache.druid.data.input.InputRowSchema;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.theta.SketchHolder;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GroupByTestColumnSelectorFactory;
import org.apache.druid.query.groupby.epinephelinae.GrouperTestUtil;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
public class OldApiSketchAggregationTest extends InitializedNullHandlingTest
{
  private AggregationTestHelper helper;

  @TempDir
  private File tempFolder;

  public void initOldApiSketchAggregationTest(final GroupByQueryConfig config)
  {
    OldApiSketchModule sm = new OldApiSketchModule();
    sm.configure(null);

    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelperWithTempDir(
        sm.getJacksonModules(),
        config,
        tempFolder
    );
  }

  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config});
    }
    return constructors;
  }

  @AfterEach
  public void teardown() throws IOException
  {
    if (helper != null) {
      helper.close();
      helper = null;
    }
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSimpleDataIngestAndQuery(final GroupByQueryConfig config) throws Exception
  {
    initOldApiSketchAggregationTest(config);
    final GroupByQuery groupByQuery = GroupByQuery.builder()
        .setDataSource("test_datasource")
        .setGranularity(Granularities.ALL)
        .setInterval(Intervals.of("2014-10-19T00:00:00.000Z/2014-10-22T00:00:00.000Z"))
        .setAggregatorSpecs(
            new OldSketchMergeAggregatorFactory("sketch_count", "pty_country", 16384, null),
            new OldSketchMergeAggregatorFactory("non_existing_col_validation", "non_existing_col", 16384, null)
        )
        .setPostAggregatorSpecs(
            new OldSketchEstimatePostAggregator(
                "sketchEstimatePostAgg",
                new FieldAccessPostAggregator("field", "sketch_count")
            ),
            new OldSketchEstimatePostAggregator(
                "sketchIntersectionPostAggEstimate",
                new OldSketchSetPostAggregator(
                    "sketchIntersectionPostAgg",
                    "INTERSECT",
                    16384,
                    Lists.newArrayList(
                        new FieldAccessPostAggregator("field1", "sketch_count"),
                        new FieldAccessPostAggregator("field2", "sketch_count")
                    )
                )
            ),
            new OldSketchEstimatePostAggregator(
                "sketchAnotBPostAggEstimate",
                new OldSketchSetPostAggregator(
                    "sketchAnotBUnionPostAgg",
                    "NOT",
                    16384,
                    Lists.newArrayList(
                        new FieldAccessPostAggregator("field1", "sketch_count"),
                        new FieldAccessPostAggregator("field2", "sketch_count")
                    )
                )
            ),
            new OldSketchEstimatePostAggregator(
                "sketchUnionPostAggEstimate",
                new OldSketchSetPostAggregator(
                    "sketchUnionPostAgg",
                    "UNION",
                    16384,
                    Lists.newArrayList(
                        new FieldAccessPostAggregator("field1", "sketch_count"),
                        new FieldAccessPostAggregator("field2", "sketch_count")
                    )
                )
            )
        )
        .build();

    final Sequence seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("simple_test_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(List.of("timestamp", "product", "pty_country")),
        List.of(
            new OldSketchBuildAggregatorFactory("pty_country", "pty_country", null),
            new OldSketchBuildAggregatorFactory("non_existing_col_validation", "non_existing_col", null)
        ),
        0,
        Granularities.NONE,
        1000,
        groupByQuery
    );

    List results = seq.toList();
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals(
        ResultRow.fromLegacyRow(
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("sketch_count", 50.0)
                    .put("sketchEstimatePostAgg", 50.0)
                    .put("sketchUnionPostAggEstimate", 50.0)
                    .put("sketchIntersectionPostAggEstimate", 50.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            groupByQuery
        ),
        results.get(0)
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSketchDataIngestAndQuery(final GroupByQueryConfig config) throws Exception
  {
    initOldApiSketchAggregationTest(config);
    final GroupByQuery groupByQuery = GroupByQuery.builder()
        .setDataSource("test_datasource")
        .setGranularity(Granularities.ALL)
        .setInterval(Intervals.of("2014-10-19T00:00:00.000Z/2014-10-22T00:00:00.000Z"))
        .setAggregatorSpecs(
            new OldSketchMergeAggregatorFactory("sids_sketch_count", "sids_sketch", 16384, null),
            new OldSketchMergeAggregatorFactory("non_existing_col_validation", "non_existing_col", 16384, null)
        )
        .setPostAggregatorSpecs(
            new OldSketchEstimatePostAggregator(
                "sketchEstimatePostAgg",
                new FieldAccessPostAggregator("field", "sids_sketch_count")
            ),
            new OldSketchEstimatePostAggregator(
                "sketchIntersectionPostAggEstimate",
                new OldSketchSetPostAggregator(
                    "sketchIntersectionPostAgg",
                    "INTERSECT",
                    16384,
                    Lists.newArrayList(
                        new FieldAccessPostAggregator("field1", "sids_sketch_count"),
                        new FieldAccessPostAggregator("field2", "sids_sketch_count")
                    )
                )
            ),
            new OldSketchEstimatePostAggregator(
                "sketchAnotBPostAggEstimate",
                new OldSketchSetPostAggregator(
                    "sketchAnotBUnionPostAgg",
                    "NOT",
                    null,
                    Lists.newArrayList(
                        new FieldAccessPostAggregator("field1", "sids_sketch_count"),
                        new FieldAccessPostAggregator("field2", "sids_sketch_count")
                    )
                )
            ),
            new OldSketchEstimatePostAggregator(
                "sketchUnionPostAggEstimate",
                new OldSketchSetPostAggregator(
                    "sketchUnionPostAgg",
                    "UNION",
                    16384,
                    Lists.newArrayList(
                        new FieldAccessPostAggregator("field1", "sids_sketch_count"),
                        new FieldAccessPostAggregator("field2", "sids_sketch_count")
                    )
                )
            )
        )
        .build();

    final Sequence seq = helper.createIndexAndRunQueryOnSegment(
        new File(OldApiSketchAggregationTest.class.getClassLoader().getResource("sketch_test_data.tsv").getFile()),
        new InputRowSchema(
            new TimestampSpec("timestamp", "yyyyMMddHH", null),
            new DimensionsSpec(DimensionsSpec.getDefaultSchemas(List.of("product"))),
            ColumnsFilter.all()
        ),
        DelimitedInputFormat.forColumns(List.of("timestamp", "product", "sketch")),
        List.of(
            new OldSketchMergeAggregatorFactory("sids_sketch", "sketch", 16384, null),
            new OldSketchMergeAggregatorFactory("non_existing_col_validation", "non_existing_col", 16384, null)
        ),
        0,
        Granularities.NONE,
        1000,
        groupByQuery
    );

    List results = seq.toList();
    Assertions.assertEquals(1, results.size());
    Assertions.assertEquals(
        ResultRow.fromLegacyRow(
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("sids_sketch_count", 50.0)
                    .put("sketchEstimatePostAgg", 50.0)
                    .put("sketchUnionPostAggEstimate", 50.0)
                    .put("sketchIntersectionPostAggEstimate", 50.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            groupByQuery
        ),
        results.get(0)
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSketchMergeAggregatorFactorySerde(final GroupByQueryConfig config) throws Exception
  {
    initOldApiSketchAggregationTest(config);
    assertAggregatorFactorySerde(new OldSketchMergeAggregatorFactory("name", "fieldName", 16, null));
    assertAggregatorFactorySerde(new OldSketchMergeAggregatorFactory("name", "fieldName", 16, false));
    assertAggregatorFactorySerde(new OldSketchMergeAggregatorFactory("name", "fieldName", 16, true));
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSketchBuildAggregatorFactorySerde(final GroupByQueryConfig config) throws Exception
  {
    initOldApiSketchAggregationTest(config);
    assertAggregatorFactorySerde(new OldSketchBuildAggregatorFactory("name", "fieldName", 16));
  }

  private void assertAggregatorFactorySerde(AggregatorFactory agg) throws Exception
  {
    Assertions.assertEquals(
        agg,
        helper.getObjectMapper().readValue(
            helper.getObjectMapper().writeValueAsString(agg),
            AggregatorFactory.class
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSketchEstimatePostAggregatorSerde(final GroupByQueryConfig config) throws Exception
  {
    initOldApiSketchAggregationTest(config);
    assertPostAggregatorSerde(
        new OldSketchEstimatePostAggregator(
            "name",
            new FieldAccessPostAggregator("name", "fieldName")
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testSketchSetPostAggregatorSerde(final GroupByQueryConfig config) throws Exception
  {
    initOldApiSketchAggregationTest(config);
    assertPostAggregatorSerde(
        new OldSketchSetPostAggregator(
            "name",
            "INTERSECT",
            null,
            Lists.newArrayList(
                new FieldAccessPostAggregator("name1", "fieldName1"),
                new FieldAccessPostAggregator("name2", "fieldName2")
            )
        )
    );
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testRelocation(final GroupByQueryConfig config)
  {
    initOldApiSketchAggregationTest(config);
    final GroupByTestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    SketchHolder sketchHolder = SketchHolder.of(Sketches.updateSketchBuilder().setNominalEntries(16).build());
    UpdateSketch updateSketch = (UpdateSketch) sketchHolder.getSketch();
    updateSketch.update(1);

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("sketch", sketchHolder)));
    SketchHolder[] holders = helper.runRelocateVerificationTest(
        new OldSketchMergeAggregatorFactory("sketch", "sketch", 16, false),
        columnSelectorFactory,
        SketchHolder.class
    );
    Assertions.assertEquals(holders[0].getEstimate(), holders[1].getEstimate(), 0);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testWithNameMerge(final GroupByQueryConfig config)
  {
    initOldApiSketchAggregationTest(config);
    OldSketchMergeAggregatorFactory factory = new OldSketchMergeAggregatorFactory("name", "fieldName", 16, null);
    Assertions.assertEquals(factory, factory.withName("name"));
    Assertions.assertEquals("newTest", factory.withName("newTest").getName());
  }


  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "{0}")
  public void testWithNameBuild(final GroupByQueryConfig config)
  {
    initOldApiSketchAggregationTest(config);
    OldSketchBuildAggregatorFactory factory = new OldSketchBuildAggregatorFactory("name", "fieldName", 16);
    Assertions.assertEquals(factory, factory.withName("name"));
    Assertions.assertEquals("newTest", factory.withName("newTest").getName());
  }

  private void assertPostAggregatorSerde(PostAggregator agg) throws Exception
  {
    Assertions.assertEquals(
        agg,
        helper.getObjectMapper().readValue(
            helper.getObjectMapper().writeValueAsString(agg),
            PostAggregator.class
        )
    );
  }
}
