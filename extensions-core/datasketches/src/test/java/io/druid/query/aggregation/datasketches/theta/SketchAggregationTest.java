/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.aggregation.datasketches.theta;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.Sketches;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import io.druid.query.select.SelectResultValue;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;

/**
 */
public class SketchAggregationTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public SketchAggregationTest()
  {
    SketchModule sm = new SketchModule();
    sm.configure(null);
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(sm.getJacksonModules(), tempFolder);
  }

  @Test
  public void testSimpleDataIngestAndGpByQuery() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("simple_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("simple_test_data_aggregators.json"),
        0,
        QueryGranularities.NONE,
        5,
        readFileFromClasspathAsString("simple_test_data_group_by_query.json")
    );

    List<Row> results = Sequences.toList(seq, Lists.<Row>newArrayList());
    Assert.assertEquals(5, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTime.parse("2014-10-19T00:00:00.000Z"),
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
                DateTime.parse("2014-10-19T00:00:00.000Z"),
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
                DateTime.parse("2014-10-19T00:00:00.000Z"),
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
                DateTime.parse("2014-10-19T00:00:00.000Z"),
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
                DateTime.parse("2014-10-19T00:00:00.000Z"),
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
        ),
        results
    );
  }

  @Test
  public void testSimpleDataIngestAndSelectQuery() throws Exception
  {
    SketchModule sm = new SketchModule();
    sm.configure(null);
    AggregationTestHelper selectQueryAggregationTestHelper = AggregationTestHelper.createSelectQueryAggregationTestHelper(
        sm.getJacksonModules(),
        tempFolder
    );

    Sequence seq = selectQueryAggregationTestHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("simple_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("simple_test_data_aggregators.json"),
        0,
        QueryGranularities.NONE,
        5000,
        readFileFromClasspathAsString("select_query.json")
    );

    Result<SelectResultValue> result = (Result<SelectResultValue>) Iterables.getOnlyElement(Sequences.toList(seq, Lists.newArrayList()));
    Assert.assertEquals(new DateTime("2014-10-20T00:00:00.000Z"), result.getTimestamp());
    Assert.assertEquals(100, result.getValue().getEvents().size());
    Assert.assertEquals("AgMDAAAazJMCAAAAAACAPzz9j7pWTMdROWGf15uY1nI=", result.getValue().getEvents().get(0).getEvent().get("pty_country"));
  }

  @Test
  public void testSketchDataIngestAndGpByQuery() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(SketchAggregationTest.class.getClassLoader().getResource("sketch_test_data.tsv").getFile()),
        readFileFromClasspathAsString("sketch_test_data_record_parser.json"),
        readFileFromClasspathAsString("sketch_test_data_aggregators.json"),
        0,
        QueryGranularities.NONE,
        5,
        readFileFromClasspathAsString("sketch_test_data_group_by_query.json")
    );

    List<Row> results = Sequences.toList(seq, Lists.<Row>newArrayList());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        new MapBasedRow(
            DateTime.parse("2014-10-19T00:00:00.000Z"),
            ImmutableMap
                .<String, Object>builder()
                .put("sids_sketch_count", 50.0)
                .put("sids_sketch_count_with_err", 
                    new SketchEstimateWithErrorBounds(50.0, 50.0, 50.0, 2))
                .put("sketchEstimatePostAgg", 50.0)
                .put("sketchEstimatePostAggWithErrorBounds", 
                    new SketchEstimateWithErrorBounds(50.0, 50.0, 50.0, 2))
                .put("sketchUnionPostAggEstimate", 50.0)
                .put("sketchIntersectionPostAggEstimate", 50.0)
                .put("sketchAnotBPostAggEstimate", 0.0)
                .put("non_existing_col_validation", 0.0)
                .build()
        ),
        results.get(0)
    );
  }

  @Test
  public void testThetaCardinalityOnSimpleColumn() throws Exception
  {
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(SketchAggregationTest.class.getClassLoader().getResource("simple_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser2.json"),
        "["
        + "  {"
        + "    \"type\": \"count\","
        + "    \"name\": \"count\""
        + "  }"
        + "]",
        0,
        QueryGranularities.NONE,
        5,
        readFileFromClasspathAsString("simple_test_data_group_by_query.json")
    );

    List<Row> results = Sequences.toList(seq, Lists.<Row>newArrayList());
    Assert.assertEquals(5, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTime.parse("2014-10-19T00:00:00.000Z"),
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
                DateTime.parse("2014-10-19T00:00:00.000Z"),
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
                DateTime.parse("2014-10-19T00:00:00.000Z"),
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
                DateTime.parse("2014-10-19T00:00:00.000Z"),
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
                DateTime.parse("2014-10-19T00:00:00.000Z"),
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
        ),
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
  public void testSketchMergeFinalization() throws Exception
  {
    Sketch sketch = Sketches.updateSketchBuilder().build(128);

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
  }

  @Test
  public void testSketchSetPostAggregatorSerde() throws Exception
  {
    assertPostAggregatorSerde(
        new SketchSetPostAggregator(
            "name",
            "INTERSECT",
            null,
            Lists.<PostAggregator>newArrayList(
                new FieldAccessPostAggregator("name1", "fieldName1"),
                new FieldAccessPostAggregator("name2", "fieldName2")
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
    Sequence<Row> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("retention_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("simple_test_data_aggregators.json"),
        0,
        QueryGranularities.NONE,
        5,
        readFileFromClasspathAsString("retention_test_data_group_by_query.json")
    );

    List<Row> results = Sequences.toList(seq, Lists.<Row>newArrayList());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTime.parse("2014-10-19T00:00:00.000Z"),
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
        ),
        results
    );
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

  public final static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(SketchAggregationTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
