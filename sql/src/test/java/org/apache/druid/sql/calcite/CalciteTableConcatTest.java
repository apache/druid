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
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

public class CalciteTableConcatTest extends BaseCalciteQueryTest
{
  @Rule(order = 0)
  public NotYetSupportedProcessor negativeTestProcessor = new NotYetSupportedProcessor();

  @Ignore
  @Test
  public void testPlainSelect()
  {
    testBuilder()
        .sql("select * from foo")
        // .sql("select datasource, sum(duration) from sys.tasks group by datasource")
        .expectedResults(
            ImmutableList.of(
                new Object[] {"foo", 11L},
                new Object[] {"foo2", 22L}
            )
        )
        .run();
  }

  @Test
  public void testConcat1()
  {
    testBuilder()
        .sql("select dim1,dim4,d1,f1 from TABLE(APPEND('foo','numfoo')) u")
        // .sql("select datasource, sum(duration) from sys.tasks group by datasource")
        .expectedResults(
            ImmutableList.of(
                new Object[] {"", null, null, null},
                new Object[] {"10.1", null, null, null},
                new Object[] {"2", null, null, null},
                new Object[] {"1", null, null, null},
                new Object[] {"def", null, null, null},
                new Object[] {"abc", null, null, null},
                new Object[] {"", "a", 1.0D, 1.0F},
                new Object[] {"10.1", "a", 1.7D, 0.1F},
                new Object[] {"2", "a", 0.0D, 0.0F},
                new Object[] {"1", "b", null, null},
                new Object[] {"def", "b", null, null},
                new Object[] {"abc", "b", null, null}
            )
        )
        .run();
  }
}
