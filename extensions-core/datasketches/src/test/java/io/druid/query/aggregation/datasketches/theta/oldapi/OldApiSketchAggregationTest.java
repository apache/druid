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

package io.druid.query.aggregation.datasketches.theta.oldapi;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

import io.druid.data.input.MapBasedRow;
import io.druid.granularity.QueryGranularities;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

/**
 */
public class OldApiSketchAggregationTest
{
  private final AggregationTestHelper helper;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public OldApiSketchAggregationTest()
  {
    OldApiSketchModule sm = new OldApiSketchModule();
    sm.configure(null);

    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        sm.getJacksonModules(),
        tempFolder
    );
  }

  @Test
  public void testSimpleDataIngestAndQuery() throws Exception
  {
    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("simple_test_data.tsv").getFile()),
        readFileFromClasspathAsString("simple_test_data_record_parser.json"),
        readFileFromClasspathAsString("oldapi/old_simple_test_data_aggregators.json"),
        0,
        QueryGranularities.NONE,
        5,
        readFileFromClasspathAsString("oldapi/old_simple_test_data_group_by_query.json")
    );

    List results = Sequences.toList(seq, Lists.newArrayList());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        new MapBasedRow(
            DateTime.parse("2014-10-19T00:00:00.000Z"),
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
        results.get(0)
    );
  }

  @Test
  public void testSketchDataIngestAndQuery() throws Exception
  {
    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        new File(OldApiSketchAggregationTest.class.getClassLoader().getResource("sketch_test_data.tsv").getFile()),
        readFileFromClasspathAsString("sketch_test_data_record_parser.json"),
        readFileFromClasspathAsString("oldapi/old_sketch_test_data_aggregators.json"),
        0,
        QueryGranularities.NONE,
        5,
        readFileFromClasspathAsString("oldapi/old_sketch_test_data_group_by_query.json")
    );

    List results = Sequences.toList(seq, Lists.newArrayList());
    Assert.assertEquals(1, results.size());
    Assert.assertEquals(
        new MapBasedRow(
            DateTime.parse("2014-10-19T00:00:00.000Z"),
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
            Lists.<PostAggregator>newArrayList(
                new FieldAccessPostAggregator("name1", "fieldName1"),
                new FieldAccessPostAggregator("name2", "fieldName2")
            )
        )
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
        new File(OldApiSketchAggregationTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
