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

package org.apache.druid.query.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.topn.TopNQuery;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Set;

public class MaterializedViewUtilsTest extends InitializedNullHandlingTest
{
  private static ObjectMapper jsonMapper = TestHelper.makeJsonMapper();
  
  @Test
  public void testGetRequiredFieldsFromGroupByQuery() throws Exception
  {
    String queryStr = "{\n" +
        "  \"queryType\": \"groupBy\",\n" +
        "  \"dataSource\": \"sample_datasource\",\n" +
        "  \"granularity\": \"day\",\n" +
        "  \"dimensions\": [\"country\", \"device\"],\n" +
        "  \"limitSpec\": { \"type\": \"default\", \"limit\": 5000, \"columns\": [\"country\", \"data_transfer\"] },\n" +
        "  \"filter\": {\n" +
        "    \"type\": \"and\",\n" +
        "    \"fields\": [\n" +
        "      { \"type\": \"selector\", \"dimension\": \"carrier\", \"value\": \"AT&T\" },\n" +
        "      { \"type\": \"or\", \n" +
        "        \"fields\": [\n" +
        "          { \"type\": \"selector\", \"dimension\": \"make\", \"value\": \"Apple\" },\n" +
        "          { \"type\": \"selector\", \"dimension\": \"make\", \"value\": \"Samsung\" }\n" +
        "        ]\n" +
        "      }\n" +
        "    ]\n" +
        "  },\n" +
        "  \"aggregations\": [\n" +
        "    { \"type\": \"longSum\", \"name\": \"total_usage\", \"fieldName\": \"user_count\" },\n" +
        "    { \"type\": \"doubleSum\", \"name\": \"data_transfer\", \"fieldName\": \"data_transfer\" }\n" +
        "  ],\n" +
        "  \"postAggregations\": [\n" +
        "    { \"type\": \"arithmetic\",\n" +
        "      \"name\": \"avg_usage\",\n" +
        "      \"fn\": \"/\",\n" +
        "      \"fields\": [\n" +
        "        { \"type\": \"fieldAccess\", \"fieldName\": \"data_transfer\" },\n" +
        "        { \"type\": \"fieldAccess\", \"fieldName\": \"total_usage\" }\n" +
        "      ]\n" +
        "    }\n" +
        "  ],\n" +
        "  \"intervals\": [ \"2012-01-01T00:00:00.000/2012-01-03T00:00:00.000\" ],\n" +
        "  \"having\": {\n" +
        "    \"type\": \"greaterThan\",\n" +
        "    \"aggregation\": \"total_usage\",\n" +
        "    \"value\": 100\n" +
        "  }\n" +
        "}";
    GroupByQuery query = jsonMapper.readValue(queryStr, GroupByQuery.class);
    Set<String> fields = MaterializedViewUtils.getRequiredFields(query);
    Assert.assertEquals(
        Sets.newHashSet("country", "device", "carrier", "make", "user_count", "data_transfer"),
        fields
    );
  }
  
  @Test
  public void testGetRequiredFieldsFromTopNQuery() throws Exception
  {
    String queryStr = "{\n" +
        "  \"queryType\": \"topN\",\n" +
        "  \"dataSource\": \"sample_data\",\n" +
        "  \"dimension\": \"sample_dim\",\n" +
        "  \"threshold\": 5,\n" +
        "  \"metric\": \"count\",\n" +
        "  \"granularity\": \"all\",\n" +
        "  \"filter\": {\n" +
        "    \"type\": \"and\",\n" +
        "    \"fields\": [\n" +
        "      {\n" +
        "        \"type\": \"selector\",\n" +
        "        \"dimension\": \"dim1\",\n" +
        "        \"value\": \"some_value\"\n" +
        "      },\n" +
        "      {\n" +
        "        \"type\": \"selector\",\n" +
        "        \"dimension\": \"dim2\",\n" +
        "        \"value\": \"some_other_val\"\n" +
        "      }\n" +
        "    ]\n" +
        "  },\n" +
        "  \"aggregations\": [\n" +
        "    {\n" +
        "      \"type\": \"longSum\",\n" +
        "      \"name\": \"count\",\n" +
        "      \"fieldName\": \"count\"\n" +
        "    },\n" +
        "    {\n" +
        "      \"type\": \"doubleSum\",\n" +
        "      \"name\": \"some_metric\",\n" +
        "      \"fieldName\": \"some_metric\"\n" +
        "    }\n" +
        "  ],\n" +
        "  \"postAggregations\": [\n" +
        "    {\n" +
        "      \"type\": \"arithmetic\",\n" +
        "      \"name\": \"average\",\n" +
        "      \"fn\": \"/\",\n" +
        "      \"fields\": [\n" +
        "        {\n" +
        "          \"type\": \"fieldAccess\",\n" +
        "          \"name\": \"some_metric\",\n" +
        "          \"fieldName\": \"some_metric\"\n" +
        "        },\n" +
        "        {\n" +
        "          \"type\": \"fieldAccess\",\n" +
        "          \"name\": \"count\",\n" +
        "          \"fieldName\": \"count\"\n" +
        "        }\n" +
        "      ]\n" +
        "    }\n" +
        "  ],\n" +
        "  \"intervals\": [\n" +
        "    \"2013-08-31T00:00:00.000/2013-09-03T00:00:00.000\"\n" +
        "  ]\n" +
        "}";
    TopNQuery query = jsonMapper.readValue(queryStr, TopNQuery.class);
    Set<String> fields = MaterializedViewUtils.getRequiredFields(query);
    Assert.assertEquals(
        Sets.newHashSet("sample_dim", "dim1", "dim2", "count", "some_metric"),
        fields
    );
  }
  
  @Test
  public void testGetRequiredFieldsFromTimeseriesQuery() throws Exception
  {
    String queryStr = "{\n" +
        "  \"queryType\": \"timeseries\",\n" +
        "  \"dataSource\": \"sample_datasource\",\n" +
        "  \"granularity\": \"day\",\n" +
        "  \"descending\": \"true\",\n" +
        "  \"filter\": {\n" +
        "    \"type\": \"and\",\n" +
        "    \"fields\": [\n" +
        "      { \"type\": \"selector\", \"dimension\": \"sample_dimension1\", \"value\": \"sample_value1\" },\n" +
        "      { \"type\": \"or\",\n" +
        "        \"fields\": [\n" +
        "          { \"type\": \"selector\", \"dimension\": \"sample_dimension2\", \"value\": \"sample_value2\" },\n" +
        "          { \"type\": \"selector\", \"dimension\": \"sample_dimension3\", \"value\": \"sample_value3\" }\n" +
        "        ]\n" +
        "      }\n" +
        "    ]\n" +
        "  },\n" +
        "  \"aggregations\": [\n" +
        "    { \"type\": \"longSum\", \"name\": \"sample_name1\", \"fieldName\": \"sample_fieldName1\" },\n" +
        "    { \"type\": \"doubleSum\", \"name\": \"sample_name2\", \"fieldName\": \"sample_fieldName2\" }\n" +
        "  ],\n" +
        "  \"postAggregations\": [\n" +
        "    { \"type\": \"arithmetic\",\n" +
        "      \"name\": \"sample_divide\",\n" +
        "      \"fn\": \"/\",\n" +
        "      \"fields\": [\n" +
        "        { \"type\": \"fieldAccess\", \"name\": \"postAgg__sample_name1\", \"fieldName\": \"sample_name1\" },\n" +
        "        { \"type\": \"fieldAccess\", \"name\": \"postAgg__sample_name2\", \"fieldName\": \"sample_name2\" }\n" +
        "      ]\n" +
        "    }\n" +
        "  ],\n" +
        "  \"intervals\": [ \"2012-01-01T00:00:00.000/2012-01-03T00:00:00.000\" ]\n" +
        "}";
    TimeseriesQuery query = jsonMapper.readValue(queryStr, TimeseriesQuery.class);
    Set<String> fields = MaterializedViewUtils.getRequiredFields(query);
    Assert.assertEquals(
        Sets.newHashSet("sample_dimension1", "sample_dimension2", "sample_dimension3", "sample_fieldName1",
            "sample_fieldName2"),
        fields
    );
  }
  
  @Test
  public void testIntervalMinus()
  {
    List<Interval> intervalList1 = Lists.newArrayList(
        Intervals.of("2012-01-02T00:00:00.000/2012-01-03T00:00:00.000"),
        Intervals.of("2012-01-08T00:00:00.000/2012-01-10T00:00:00.000"),
        Intervals.of("2012-01-16T00:00:00.000/2012-01-17T00:00:00.000")
    );
    List<Interval> intervalList2 = Lists.newArrayList(
        Intervals.of("2012-01-01T00:00:00.000/2012-01-04T00:00:00.000"),
        Intervals.of("2012-01-05T00:00:00.000/2012-01-10T00:00:00.000"),
        Intervals.of("2012-01-16T00:00:00.000/2012-01-18T00:00:00.000"),
        Intervals.of("2012-01-19T00:00:00.000/2012-01-20T00:00:00.000")
    );
    
    List<Interval> result = MaterializedViewUtils.minus(intervalList2, intervalList1);
    Assert.assertEquals(
        Lists.newArrayList(
            Intervals.of("2012-01-01T00:00:00.000/2012-01-02T00:00:00.000"),
            Intervals.of("2012-01-03T00:00:00.000/2012-01-04T00:00:00.000"),
            Intervals.of("2012-01-05T00:00:00.000/2012-01-08T00:00:00.000"),
            Intervals.of("2012-01-17T00:00:00.000/2012-01-18T00:00:00.000"),
            Intervals.of("2012-01-19T00:00:00.000/2012-01-20T00:00:00.000")
        ),
        result
    );
  }
}
