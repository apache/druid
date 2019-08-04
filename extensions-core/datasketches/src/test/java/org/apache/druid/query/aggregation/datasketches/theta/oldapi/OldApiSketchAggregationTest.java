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
import com.google.common.io.Files;
import com.yahoo.sketches.theta.Sketches;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.druid.data.input.MapBasedRow;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.datasketches.theta.SketchHolder;
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
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 *
 */
@RunWith(Parameterized.class)
public class OldApiSketchAggregationTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public OldApiSketchAggregationTest(final GroupByQueryConfig config)
  {
    OldApiSketchModule sm = new OldApiSketchModule();
    sm.configure(null);

    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        sm.getJacksonModules(),
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
  public void testSimpleDataIngestAndQuery() throws Exception
  {
    final String groupByQueryString = readFileFromClasspathAsString("oldapi/old_simple_test_data_group_by_query.json");
    final GroupByQuery groupByQuery = (GroupByQuery) helper.getObjectMapper()
                                                           .readValue(groupByQueryString, Query.class);

    final Sequence seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("simple_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("oldapi/old_simple_test_data_aggregators.json"),
        0,
        Granularities.NONE,
        1000,
        groupByQueryString
    );

    List results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
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

  @Test
  public void testSketchDataIngestAndQuery() throws Exception
  {
    final String groupByQueryString = readFileFromClasspathAsString("oldapi/old_sketch_test_data_group_by_query.json");
    final GroupByQuery groupByQuery = (GroupByQuery) helper.getObjectMapper()
                                                           .readValue(groupByQueryString, Query.class);

    final Sequence seq = helper.createIndexAndRunQueryOnSegment(
        new File(OldApiSketchAggregationTest.class.getClassLoader().getResource("sketch_test_data.tsv").getFile()),
        readFileFromClasspathAsString("sketch_test_data_record_parser.json"),
        readFileFromClasspathAsString("oldapi/old_sketch_test_data_aggregators.json"),
        0,
        Granularities.NONE,
        1000,
        groupByQueryString
    );

    List results = seq.toList();
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
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

  @Test
  public void testSketchMergeAggregatorFactorySerde() throws Exception
  {
    assertAggregatorFactorySerde(new OldSketchMergeAggregatorFactory("name", "fieldName", 16, null));
    assertAggregatorFactorySerde(new OldSketchMergeAggregatorFactory("name", "fieldName", 16, false));
    assertAggregatorFactorySerde(new OldSketchMergeAggregatorFactory("name", "fieldName", 16, true));
  }

  @Test
  public void testSketchBuildAggregatorFactorySerde() throws Exception
  {
    assertAggregatorFactorySerde(new OldSketchBuildAggregatorFactory("name", "fieldName", 16));
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
        new OldSketchEstimatePostAggregator(
            "name",
            new FieldAccessPostAggregator("name", "fieldName")
        )
    );
  }

  @Test
  public void testSketchSetPostAggregatorSerde() throws Exception
  {
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

  @Test
  public void testRelocation()
  {
    final TestColumnSelectorFactory columnSelectorFactory = GrouperTestUtil.newColumnSelectorFactory();
    SketchHolder sketchHolder = SketchHolder.of(Sketches.updateSketchBuilder().setNominalEntries(16).build());
    UpdateSketch updateSketch = (UpdateSketch) sketchHolder.getSketch();
    updateSketch.update(1);

    columnSelectorFactory.setRow(new MapBasedRow(0, ImmutableMap.of("sketch", sketchHolder)));
    SketchHolder[] holders = helper.runRelocateVerificationTest(
        new OldSketchMergeAggregatorFactory("sketch", "sketch", 16, false),
        columnSelectorFactory,
        SketchHolder.class
    );
    Assert.assertEquals(holders[0].getEstimate(), holders[1].getEstimate(), 0);
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

  public static final String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(OldApiSketchAggregationTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
