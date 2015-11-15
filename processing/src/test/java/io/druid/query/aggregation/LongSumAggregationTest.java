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

package io.druid.query.aggregation;

import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import io.druid.data.input.MapBasedRow;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.AggregatorsModule;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 */
public class LongSumAggregationTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private File dataFile;
  private String parseSpec;
  private String query;

  @Before
  public void setup() throws Exception
  {
    String line1 = "2011-01-12T00:00:00.000Z\tproduct1\t2";
    String line2 = "2011-01-14T00:00:00.000Z\tproduct2\t3";

    List<String> lines = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      lines.add(line1);
      lines.add(line2);
    }

    this.dataFile = tempFolder.newFile();
    FileUtils.writeLines(dataFile, "UTF-8", lines, "\n");

    this.parseSpec = "{"
                       + "\"type\" : \"string\","
                       + "\"parseSpec\" : {"
                       + "    \"format\" : \"tsv\","
                       + "    \"timestampSpec\" : {"
                       + "        \"column\" : \"timestamp\","
                       + "        \"format\" : \"auto\""
                       + "},"
                       + "    \"dimensionsSpec\" : {"
                       + "        \"dimensions\": [\"product\"],"
                       + "        \"dimensionExclusions\" : [],"
                       + "        \"spatialDimensions\" : []"
                       + "    },"
                       + "    \"columns\": [\"timestamp\", \"product\", \"quantity\"]"
                       + "  }"
                       + "}";

    this.query = "{"
                   + "\"queryType\": \"groupBy\","
                   + "\"dataSource\": \"test_datasource\","
                   + "\"granularity\": \"ALL\","
                   + "\"dimensions\": [],"
                   + "\"aggregations\": ["
                   + "  { \"type\": \"longSum\", \"name\": \"quantity\", \"fieldName\": \"quantity\" }"
                   + "],"
                   + "\"intervals\": [ \"1970/2050\" ]"
                   + "}";
  }

  @Test
  public void testIngestAndQueryNullExponent() throws Exception
  {
    AggregationTestHelper helper = new AggregationTestHelper(Lists.newArrayList(new AggregatorsModule()), tempFolder);

    String metricSpec = "[{"
                        + "\"type\": \"longSum\","
                        + "\"name\": \"quantity\","
                        + "\"fieldName\": \"quantity\""
                        + "}]";

    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        dataFile,
        parseSpec,
        metricSpec,
        0,
        QueryGranularity.NONE,
        50000,
        query
    );

    MapBasedRow row = (MapBasedRow) Sequences.toList(seq, Lists.newArrayList()).get(0);
    Assert.assertEquals(25, row.getLongMetric("quantity"));
  }

  @Test
  public void testIngestAndQuerySquareExponent() throws Exception
  {
    AggregationTestHelper helper = new AggregationTestHelper(Lists.newArrayList(new AggregatorsModule()), tempFolder);

    String metricSpec = "[{"
                        + "\"type\": \"longSum\","
                        + "\"name\": \"quantity\","
                        + "\"fieldName\": \"quantity\","
                        + "\"exponent\": \"2\""
                        + "}]";

    Sequence seq = helper.createIndexAndRunQueryOnSegment(
        dataFile,
        parseSpec,
        metricSpec,
        0,
        QueryGranularity.NONE,
        50000,
        query
    );

    MapBasedRow row = (MapBasedRow) Sequences.toList(seq, Lists.newArrayList()).get(0);
    Assert.assertEquals(65, row.getLongMetric("quantity"));
  }
}
