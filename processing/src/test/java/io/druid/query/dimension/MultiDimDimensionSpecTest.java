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

package io.druid.query.dimension;

import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularities;
import io.druid.query.Result;
import io.druid.query.aggregation.AggregationTestHelper;
import io.druid.query.search.SearchResultValue;
import io.druid.query.search.search.SearchHit;
import io.druid.query.select.EventHolder;
import io.druid.query.select.SelectResultValue;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;

public class MultiDimDimensionSpecTest
{
  private final AggregationTestHelper selectQueryHelper, groupByQueryHelper, searchQueryHelper;

  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  public MultiDimDimensionSpecTest()
  {
    selectQueryHelper = AggregationTestHelper.createSelectQueryAggregationTestHelper(ImmutableList.<Module>of(), temporaryFolder);
    groupByQueryHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(ImmutableList.<Module>of(), temporaryFolder);
    searchQueryHelper = AggregationTestHelper.createSearchQueryAggregationTestHelper(ImmutableList.<Module>of(), temporaryFolder);
  }

  @Test
  public void testSimpleDataIngestionAndSelect() throws Exception
  {
    Sequence seq = selectQueryHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("multidim_data.tsv").getFile()),
        readFileFromClasspathAsString("multidim_data_record_parser.json"),
        readFileFromClasspathAsString("multidim_data_aggregators.json"),
        0,
        QueryGranularities.NONE,
        5000,
        readFileFromClasspathAsString("multidim_data_select_query.json")
    );

    Result<SelectResultValue> result = (Result<SelectResultValue>) Iterables.getOnlyElement(Sequences.toList(seq, Lists.newArrayList()));
    List<EventHolder> events = result.getValue().getEvents();
    Assert.assertEquals(474, events.size());

    int countMapped = 0;
    for (EventHolder eventHolder: events)
    {
      Object value = eventHolder.getEvent().get("multilookup");
      if ("xxx".equals(value))
      {
        countMapped++;
      }
    }

    Assert.assertEquals(2, countMapped);
  }

  @Test
  public void testSimpleDataIngestionAndGroupBy() throws Exception
  {
    Sequence<Row> seq = groupByQueryHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("multidim_data.tsv").getFile()),
        readFileFromClasspathAsString("multidim_data_record_parser.json"),
        readFileFromClasspathAsString("multidim_data_aggregators.json"),
        0,
        QueryGranularities.NONE,
        5000,
        readFileFromClasspathAsString("multidim_data_groupby_query.json")
    );

    List<Row> results = Sequences.toList(seq, Lists.<Row>newArrayList());

    Assert.assertEquals(2, results.size());
    Assert.assertEquals(
        ImmutableList.of(
            new MapBasedRow(
                DateTime.parse("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("count", 472L)
                    .put("multilookup", "a")
                    .build()
            ),
            new MapBasedRow(
                DateTime.parse("2014-10-19T00:00:00.000Z"),
                ImmutableMap
                    .<String, Object>builder()
                    .put("count", 2L)
                    .put("multilookup", "xxx")
                    .build()
            )
        ),
        results
    );
  }

  @Test
  public void testSimpleDataIngestionAndSearch() throws Exception
  {
    Sequence seq = searchQueryHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("multidim_data.tsv").getFile()),
        readFileFromClasspathAsString("multidim_data_record_parser.json"),
        readFileFromClasspathAsString("multidim_data_aggregators.json"),
        0,
        QueryGranularities.NONE,
        5000,
        readFileFromClasspathAsString("multidim_data_search_query.json")
    );

    Result<SearchResultValue> result = (Result<SearchResultValue>) Iterables.getOnlyElement(Sequences.toList(seq, Lists.newArrayList()));
    SearchHit hit = result.getValue().getValue().get(0);

    Assert.assertEquals("multilookup", hit.getDimension());
    Assert.assertEquals("xxx", hit.getValue());
    Assert.assertEquals(15L, hit.getCount().longValue());
  }

  public final static String readFileFromClasspathAsString(String fileName) throws IOException
  {
    return Files.asCharSource(
        new File(MultiDimDimensionSpecTest.class.getClassLoader().getResource(fileName).getFile()),
        Charset.forName("UTF-8")
    ).read();
  }
}
