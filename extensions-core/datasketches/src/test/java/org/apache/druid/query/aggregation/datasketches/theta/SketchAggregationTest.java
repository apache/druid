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

package org.apache.druid.query.aggregation.datasketches.theta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import org.apache.datasketches.Family;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.datasketches.theta.Union;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.Aggregator;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.GrouperTestUtil;
import org.apache.druid.query.groupby.epinephelinae.TestColumnSelectorFactory;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
@RunWith(Parameterized.class)
public class SketchAggregationTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public SketchAggregationTest(final GroupByQueryConfig config)
  {
    SketchModule.registerSerde();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        new SketchModule().getJacksonModules(),
        config,
        tempFolder
    );
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

  @After
  public void teardown() throws IOException
  {
    helper.close();
  }

  @Test
  public void testSketchDataIngestAndGpByQuery() throws Exception
  {
    final String groupByQueryString = readFileFromClasspathAsString("sketch_test_data_group_by_query.json");
    final GroupByQuery groupByQuery = (GroupByQuery) helper.getObjectMapper()
                                                           .readValue(groupByQueryString, Query.class);

    final Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(SketchAggregationTest.class.getClassLoader().getResource("sketch_test_data.tsv").getFile()),
        readFileFromClasspathAsString("sketch_test_data_record_parser.json"),
        readFileFromClasspathAsString("sketch_test_data_aggregators.json"),
        0,
        Granularities.NONE,
        1000,
        groupByQueryString
    );

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        ResultRow.fromLegacyRow(
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("sids_sketch_count", 50.0)
                    .put(
                        "sids_sketch_count_with_err",
                        new SketchEstimateWithErrorBounds(50.0, 50.0, 50.0, 2)
                    )
                    .put("sketchEstimatePostAgg", 50.0)
                    .put(
                        "sketchEstimatePostAggWithErrorBounds",
                        new SketchEstimateWithErrorBounds(50.0, 50.0, 50.0, 2)
                    )
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

  @Test
  public void testEmptySketchAggregateCombine() throws Exception
  {
    final String groupByQueryString = readFileFromClasspathAsString("empty_sketch_group_by_query.json");
    final GroupByQuery groupByQuery = (GroupByQuery) helper.getObjectMapper()
                                                           .readValue(groupByQueryString, Query.class);

    final Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(SketchAggregationTest.class.getClassLoader().getResource("empty_sketch_data.tsv").getFile()),
        readFileFromClasspathAsString("empty_sketch_data_record_parser.json"),
        readFileFromClasspathAsString("empty_sketch_test_data_aggregators.json"),
        0,
        Granularities.NONE,
        5,
        groupByQueryString
    );

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        ResultRow.fromLegacyRow(
            new MapBasedRow(
                DateTimes.of("2019-07-14T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_b")
                    .put("sketch_count", 0.0)
                    .build()
            ),
            groupByQuery
        ),
        results.get(0)
    );
  }

  @Test
  public void testThetaCardinalityOnSimpleColumn() throws Exception
  {
    final String groupByQueryString = readFileFromClasspathAsString("simple_test_data_group_by_query.json");
    final GroupByQuery groupByQuery = (GroupByQuery) helper.getObjectMapper()
                                                           .readValue(groupByQueryString, Query.class);

    final Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(SketchAggregationTest.class.getClassLoader().getResource("simple_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser2.json"),
        "["
        + "  {"
        + "    \"type\": \"count\","
        + "    \"name\": \"count\""
        + "  }"
        + "]",
        0,
        Granularities.NONE,
        1000,
        groupByQueryString
    );

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(5, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_3")
                    .put("sketch_count", 38.0)
                    .put("sketchEstimatePostAgg", 38.0)
                    .put("sketchUnionPostAggEstimate", 38.0)
                    .put("sketchIntersectionPostAggEstimate", 38.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_1")
                    .put("sketch_count", 42.0)
                    .put("sketchEstimatePostAgg", 42.0)
                    .put("sketchUnionPostAggEstimate", 42.0)
                    .put("sketchIntersectionPostAggEstimate", 42.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_2")
                    .put("sketch_count", 42.0)
                    .put("sketchEstimatePostAgg", 42.0)
                    .put("sketchUnionPostAggEstimate", 42.0)
                    .put("sketchIntersectionPostAggEstimate", 42.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_4")
                    .put("sketch_count", 42.0)
                    .put("sketchEstimatePostAgg", 42.0)
                    .put("sketchUnionPostAggEstimate", 42.0)
                    .put("sketchIntersectionPostAggEstimate", 42.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            ),
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_5")
                    .put("sketch_count", 42.0)
                    .put("sketchEstimatePostAgg", 42.0)
                    .put("sketchUnionPostAggEstimate", 42.0)
                    .put("sketchIntersectionPostAggEstimate", 42.0)
                    .put("sketchAnotBPostAggEstimate", 0.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            )
        ).stream().map(row -> ResultRow.fromLegacyRow(row, groupByQuery)).collect(Collectors.toList()),
        results
    );
  }

  @Test
  public void testSketchMergeAggregatorFactorySerde() throws Exception
  {
    assertAggregatorFactorySerde(new SketchMergeAggregatorFactory("name", "fieldName", 16, null, null, null));
    assertAggregatorFactorySerde(new SketchMergeAggregatorFactory("name", "fieldName", 16, false, true, null));
    assertAggregatorFactorySerde(new SketchMergeAggregatorFactory("name", "fieldName", 16, true, false, null));
    assertAggregatorFactorySerde(new SketchMergeAggregatorFactory("name", "fieldName", 16, true, false, 2));
  }

  @Test
  public void testSketchMergeFinalization()
  {
    SketchHolder sketch = SketchHolder.of(Sketches.updateSketchBuilder().setNominalEntries(128).build());

    SketchMergeAggregatorFactory agg = new SketchMergeAggregatorFactory("name", "fieldName", 16, null, null, null);
    Assert.assertEquals(0.0, ((Double) agg.finalizeComputation(sketch)).doubleValue(), 0.0001);

    agg = new SketchMergeAggregatorFactory("name", "fieldName", 16, true, null, null);
    Assert.assertEquals(0.0, ((Double) agg.finalizeComputation(sketch)).doubleValue(), 0.0001);

    agg = new SketchMergeAggregatorFactory("name", "fieldName", 16, false, null, null);
    Assert.assertEquals(sketch, agg.finalizeComputation(sketch));

    agg = new SketchMergeAggregatorFactory("name", "fieldName", 16, true, null, 2);
    SketchEstimateWithErrorBounds est = (SketchEstimateWithErrorBounds) agg.finalizeComputation(sketch);
    Assert.assertEquals(0.0, est.getEstimate(), 0.0001);
    Assert.assertEquals(0.0, est.getHighBound(), 0.0001);
    Assert.assertEquals(0.0, est.getLowBound(), 0.0001);
    Assert.assertEquals(2, est.getNumStdDev());

  }

  private void assertAggregatorFactorySerde(AggregatorFactory agg) throws Exception
  {
    Assert.assertEquals(
        agg,
        helper.getObjectMapper().readValue(
            helper.getObjectMapper().writeValueAsString(agg),
            AggregatorFactory.class
        )
    );
  }

  @Test
  public void testSketchEstimatePostAggregatorSerde() throws Exception
  {
    assertPostAggregatorSerde(
        new SketchEstimatePostAggregator(
            "name",
            new FieldAccessPostAggregator("name", "fieldName"),
            null
        )
    );

    assertPostAggregatorSerde(
        new SketchEstimatePostAggregator(
            "name",
            new FieldAccessPostAggregator("name", "fieldName"),
            2
        )
    );

    assertPostAggregatorSerde(
        new SketchEstimatePostAggregator(
            "name",
            new SketchConstantPostAggregator("name", "AgMDAAAazJMCAAAAAACAPzz9j7pWTMdROWGf15uY1nI="),
            null
        )
    );
  }

  @Test
  public void testSketchSetPostAggregatorSerde() throws Exception
  {
    assertPostAggregatorSerde(
        new SketchSetPostAggregator(
            "name",
            "INTERSECT",
            null,
            Lists.newArrayList(
                new FieldAccessPostAggregator("name1", "fieldName1"),
                new FieldAccessPostAggregator("name2", "fieldName2")
            )
        )
    );

    assertPostAggregatorSerde(
        new SketchSetPostAggregator(
            "name",
            "INTERSECT",
            null,
            Lists.newArrayList(
                new FieldAccessPostAggregator("name1", "fieldName1"),
                new SketchConstantPostAggregator("name2", "AgMDAAAazJMCAAAAAACAPzz9j7pWTMdROWGf15uY1nI=")
            )
        )
    );
  }

  @Test
  public void testCacheKey()
  {
    final SketchMergeAggregatorFactory factory1 = new SketchMergeAggregatorFactory(
        "name",
        "fieldName",
        16,
        null,
        null,
        null
    );
    final SketchMergeAggregatorFactory factory2 = new SketchMergeAggregatorFactory(
        "name",
        "fieldName",
        16,
        null,
        null,
        null
    );
    final SketchMergeAggregatorFactory factory3 = new SketchMergeAggregatorFactory(
        "name",
        "fieldName",
        32,
        null,
        null,
        null
    );

    Assert.assertTrue(Arrays.equals(factory1.getCacheKey(), factory2.getCacheKey()));
    Assert.assertFalse(Arrays.equals(factory1.getCacheKey(), factory3.getCacheKey()));
  }

  @Test
  public void testRetentionDataIngestAndGpByQuery() throws Exception
  {
    final String groupByQueryString = readFileFromClasspathAsString("retention_test_data_group_by_query.json");
    final GroupByQuery groupByQuery = (GroupByQuery) helper.getObjectMapper()
                                                           .readValue(groupByQueryString, Query.class);

    final Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("retention_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("simple_test_data_aggregators.json"),
        0,
        Granularities.NONE,
        5,
        groupByQueryString
    );

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTimes.of("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("product", "product_1")
                    .put("p1_unique_country_day_1", 20.0)
                    .put("p1_unique_country_day_2", 20.0)
                    .put("p1_unique_country_day_3", 10.0)
                    .put("sketchEstimatePostAgg", 20.0)
                    .put("sketchIntersectionPostAggEstimate1", 10.0)
                    .put("sketchIntersectionPostAggEstimate2", 5.0)
                    .put("non_existing_col_validation", 0.0)
                    .build()
            )
        ).stream().map(row -> ResultRow.fromLegacyRow(row, groupByQuery)).collect(Collectors.toList()),
        results
    );
  }

  @Test
  public void testSketchAggregatorFactoryComparator()
  {
    Comparator<Object> comparator = SketchHolder.COMPARATOR;
    Assert.assertEquals(0, comparator.compare(null, null));

    Union union1 = (Union) SetOperation.builder().setNominalEntries(1 << 4).build(Family.UNION);
    union1.update("a");
    union1.update("b");
    Sketch sketch1 = union1.getResult();

    Assert.assertEquals(-1, comparator.compare(null, SketchHolder.of(sketch1)));
    Assert.assertEquals(1, comparator.compare(SketchHolder.of(sketch1), null));

    Union union2 = (Union) SetOperation.builder().setNominalEntries(1 << 4).build(Family.UNION);
    union2.update("a");
    union2.update("b");
    union2.update("c");
    Sketch sketch2 = union2.getResult();

    Assert.assertEquals(-1, comparator.compare(SketchHolder.of(sketch1), SketchHolder.of(sketch2)));
    Assert.assertEquals(-1, comparator.compare(SketchHolder.of(sketch1), SketchHolder.of(union2)));
    Assert.assertEquals(1, comparator.compare(SketchHolder.of(sketch2), SketchHolder.of(sketch1)));
    Assert.assertEquals(1, comparator.compare(SketchHolder.of(sketch2), SketchHolder.of(union1)));
    Assert.assertEquals(1, comparator.compare(SketchHolder.of(union2), SketchHolder.of(union1)));
    Assert.assertEquals(1, comparator.compare(SketchHolder.of(union2), SketchHolder.of(sketch1)));
  }

  @Test
  public void testRelocation()
  {
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    SketchHolder sketchHolder = SketchHolder.of(Sketches.updateSketchBuilder().setNominalEntries(16).build());
    UpdateSketch updateSketch = (UpdateSketch) sketchHolder.getSketch();
    updateSketch.update(1);

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("sketch", sketchHolder)));
    SketchHolder[] holders = helper.runRelocateVerificationTest(
        new SketchMergeAggregatorFactory("sketch", "sketch", 16, false, true, 2),
        columnSelectorFactory,
        SketchHolder.class
    );
    Assert.assertEquals(holders[0].getEstimate(), holders[1].getEstimate(), 0);
  }

  @Test
  public void testUpdateUnionWithNullInList()
  {
    List<String> value = new ArrayList<>();
    value.add("foo");
    value.add(null);
    value.add("bar");
    List[] columnValues = new List[]{value};
    final TestObjectColumnSelector selector = new TestObjectColumnSelector(columnValues);
    final Aggregator agg = new SketchAggregator(selector, 4096);
    agg.aggregate();
    Assert.assertFalse(agg.isNull());
    Assert.assertNotNull(agg.get());
    Assert.assertTrue(agg.get() instanceof SketchHolder);
    Assert.assertEquals(2, ((SketchHolder) agg.get()).getEstimate(), 0);
    Assert.assertNotNull(((SketchHolder) agg.get()).getSketch());
    Assert.assertEquals(2, ((SketchHolder) agg.get()).getSketch().getEstimate(), 0);
  }

  private void assertPostAggregatorSerde(PostAggregator agg) throws Exception
  {
    Assert.assertEquals(
        agg,
        helper.getObjectMapper().readValue(
            helper.getObjectMapper().writeValueAsString(agg),
            PostAggregator.class
        )
    );
  }

  public static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(SketchAggregationTest.class.getClassLoader().getResource(fileName).getFile()),
        StandardCharsets.UTF_8
    ).read();
  }
}
