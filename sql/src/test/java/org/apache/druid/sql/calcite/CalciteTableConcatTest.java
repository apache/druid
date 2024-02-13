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

package org.apache.druid.sql.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.druid.sql.calcite.NotYetSupported.NotYetSupportedProcessor;
import org.junit.Rule;
import org.junit.Test;

public class CalciteTableConcatTest extends BaseCalciteQueryTest
{
  @Rule(order = 0)
  public NotYetSupportedProcessor negativeTestProcessor = new NotYetSupportedProcessor();

  @Test
  public void testTasksSum1()
  {
    testBuilder()
      .sql("select * from foo")
//    .sql("select datasource, sum(duration) from sys.tasks group by datasource")
        .expectedResults(ImmutableList.of(
            new Object[]{"foo", 11L},
            new Object[]{"foo2", 22L}
        ))
        .run();
  }
  @Test
  public void testTasksSum()
  {
    testBuilder()
      .sql("select * from TABLE(APPEND('foo','numfoo')) u")
//    .sql("select datasource, sum(duration) from sys.tasks group by datasource")
        .expectedResults(ImmutableList.of(
            new Object[]{"foo", 11L},
            new Object[]{"foo2", 22L}
        ))
        .run();
  }
}
