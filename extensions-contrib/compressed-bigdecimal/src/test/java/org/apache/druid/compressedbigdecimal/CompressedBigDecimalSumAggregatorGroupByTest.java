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

package org.apache.druid.compressedbigdecimal;

import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


public class CompressedBigDecimalSumAggregatorGroupByTest extends CompressedBigDecimalAggregatorGroupByTestBase
{
  public CompressedBigDecimalSumAggregatorGroupByTest(
      GroupByQueryConfig config,
      CompressedBigDecimalGroupByQueryConfig cbdGroupByQueryConfig
  )
  {
    super(config, cbdGroupByQueryConfig);
  }

  /**
   * Constructor feeder.
   *
   * @return constructors
   */
  @Parameterized.Parameters(name = "{0}")
  public static Collection<?> constructorFeeder()
  {
    List<Object[]> constructors = new ArrayList<>();
    CompressedBigDecimalGroupByQueryConfig cbdGroupByQueryConfig = new CompressedBigDecimalGroupByQueryConfig(
        "bd_sum_test_groupby_query.json",
        "bd_sum_test_aggregators.json",
        "15000000010.000000005",
        "10000000010.000000000",
        "15000000010.500000000"
    );
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      constructors.add(new Object[]{config, cbdGroupByQueryConfig});
    }
    return constructors;
  }
}
