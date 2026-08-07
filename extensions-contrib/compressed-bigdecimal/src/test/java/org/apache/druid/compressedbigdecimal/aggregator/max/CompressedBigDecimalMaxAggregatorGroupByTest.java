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

package org.apache.druid.compressedbigdecimal.aggregator.max;

import org.apache.druid.compressedbigdecimal.CompressedBigDecimalGroupByQueryConfig;
import org.apache.druid.compressedbigdecimal.aggregator.CompressedBigDecimalAggregatorGroupByTestBase;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class CompressedBigDecimalMaxAggregatorGroupByTest extends CompressedBigDecimalAggregatorGroupByTestBase
{
  @ParameterizedTest
  @MethodSource("constructorFeeder")
  public void testIngestAndGroupByAllQuery(
      GroupByQueryConfig config,
      CompressedBigDecimalGroupByQueryConfig cbdGroupByQueryConfig
  ) throws Exception
  {
    super.testIngestAndGroupByAllQuery(config, cbdGroupByQueryConfig);
  }

  /**
   * Constructor feeder.
   *
   * @return constructors
   */
  public static Collection<?> constructorFeeder()
  {
    List<Object[]> constructors = new ArrayList<>();
    CompressedBigDecimalGroupByQueryConfig cbdGroupByQueryConfig = new CompressedBigDecimalGroupByQueryConfig(
        List.of(new CompressedBigDecimalMaxAggregatorFactory("bigDecimalRevenue", "revenue", 3, 9, null)),
        GroupByQuery.builder()
                    .setDataSource("test_datasource")
                    .setGranularity(Granularities.ALL)
                    .setInterval("2017-01-01T00:00:00.000Z/P1D")
                    .setAggregatorSpecs(
                        new CompressedBigDecimalMaxAggregatorFactory("cbdRevenueFromString", "revenue", 3, 9, null),
                        new CompressedBigDecimalMaxAggregatorFactory("cbdRevenueFromLong", "longRevenue", 3, 9, null),
                        new CompressedBigDecimalMaxAggregatorFactory("cbdRevenueFromDouble", "doubleRevenue", 3, 9, null)
                    )
                    .build(),
        "9999999999.000000000",
        "9999999999.000000000",
        "9999999999.000000000"
    );
    for (GroupByQueryConfig config : testConfigs()) {
      constructors.add(new Object[]{config, cbdGroupByQueryConfig});
    }
    return constructors;
  }
}
