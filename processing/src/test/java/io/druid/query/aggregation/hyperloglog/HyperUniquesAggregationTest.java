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

package io.druid.query.aggregation.hyperloglog;

import com.google.common.collect.Lists;
import io.druid.data.input.MapBasedRow;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.AggregatorsModule;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.aggregation.AggregationTestHelper;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

public class HyperUniquesAggregationTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testIngestAndQuery() throws Exception
  {
    AggregationTestHelper helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        Lists.newArrayList(new AggregatorsModule()),
        tempFolder
    );

    String metricSpec = "[{"
                        + "\"type\": \"hyperUnique\","
                        + "\"name\": \"index_hll\","
                        + "\"fieldName\": \"market\""
                        + "}]";

    String parseSpec = "{"
                       + "\"type\" : \"string\","
                       + "\"parseSpec\" : {"
                       + "    \"format\" : \"tsv\","
                       + "    \"timestampSpec\" : {"
                       + "        \"column\" : \"timestamp\","
                       + "        \"format\" : \"auto\""
                       + "},"
                       + "    \"dimensionsSpec\" : {"
                       + "        \"dimensions\": [],"
                       + "        \"dimensionExclusions\" : [],"
                       + "        \"spatialDimensions\" : []"
                       + "    },"
                       + "    \"columns\": [\"timestamp\", \"market\", \"quality\", \"placement\", \"placementish\", \"index\"]"
                       + "  }"
                       + "}";

    String query = "{"
                   + "\"queryType\": \"groupBy\","
                   + "\"dataSource\": \"test_datasource\","
                   + "\"granularity\": \"ALL\","
                   + "\"dimensions\": [],"
                   + "\"aggregations\": ["
                   + "  { \"type\": \"hyperUnique\", \"name\": \"index_hll\", \"fieldName\": \"index_hll\" }"
                   + "],"
                   + "\"postAggregations\": ["
                   + "  { \"type\": \"hyperUniqueCardinality\", \"name\": \"index_unique_count\", \"fieldName\": \"index_hll\" }"
                   + "],"
                   + "\"intervals\": [ \"1970/2050\" ]"
                   + "}";

    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("druid.sample.tsv").getFile()),
        parseSpec,
        metricSpec,
        0,
        QueryGranularities.NONE,
        50000,
        query
    );

    MapBasedRow row = (MapBasedRow) Sequences.toList(seq, Lists.newArrayList()).get(0);
    Assert.assertEquals(3.0, row.getFloatMetric("index_hll"), 0.1);
    Assert.assertEquals(3.0, row.getFloatMetric("index_unique_count"), 0.1);
  }
}
