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

package org.apache.druid.uint;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.strategy.GroupByStrategySelector;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

public abstract class DruidBaseTest
{

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();
  protected final AggregationTestHelper groupByTestHelper;
  protected final ObjectMapper jsonMapper;

  public DruidBaseTest()
  {
    UnsignedIntDruidModule module = new UnsignedIntDruidModule();
    module.configure(null);
    GroupByQueryConfig config = new GroupByQueryConfig()
    {
      @Override
      public String getDefaultStrategy()
      {
        return GroupByStrategySelector.STRATEGY_V2;
      }
    };

    groupByTestHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        module.getJacksonModules(), config, tempFolder
    );

    jsonMapper = groupByTestHelper.getObjectMapper();
  }

  public static void createSegmentFromDataFile(
      AggregationTestHelper aggregationTestHelper,
      String dataFile, File segmentDir
  ) throws Exception
  {
    aggregationTestHelper.createIndex(
        Paths.get(Objects.requireNonNull(DruidBaseTest.class.getClassLoader().getResource(dataFile))
                         .toURI()).toFile(),
        readJsonAsString("spec.json"),
        readJsonAsString("index_agg.json"),
        segmentDir,
        0,
        Granularities.NONE,
        2,
        false
    );
  }

  public static String readJsonAsString(String s) throws IOException, URISyntaxException
  {
    List<String> lines = Files.readAllLines(
        Paths
            .get(Objects.requireNonNull(DruidBaseTest.class.getClassLoader()
                                                           .getResource(
                                                               s))
                        .toURI()));
    StringBuilder builder = new StringBuilder();
    for (String line : lines) {
      if (!line.trim().startsWith("//")) {
        builder.append(line).append(" ");
      }
    }
    return builder.toString();
  }

}
