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
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.sql.calcite.NotYetSupported.NotYetSupportedProcessor;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

public class CalciteTableAppendTest extends BaseCalciteQueryTest
{
  @Rule(order = 0)
  public NotYetSupportedProcessor negativeTestProcessor = new NotYetSupportedProcessor();

  @Test
  public void testUnion()
  {
    testBuilder()
        .sql("select dim1,null as dim4 from foo union all select dim1,dim4 from numfoo")
        .expectedResults(
            ImmutableList.of(
                new Object[] {"", null},
                new Object[] {"10.1", null},
                new Object[] {"2", null},
                new Object[] {"1", null},
                new Object[] {"def", null},
                new Object[] {"abc", null},
                new Object[] {"", "a"},
                new Object[] {"10.1", "a"},
                new Object[] {"2", "a"},
                new Object[] {"1", "b"},
                new Object[] {"def", "b"},
                new Object[] {"abc", "b"}
            )
        )
        .run();
  }

  @Test
  public void testAppend2()
  {
    testBuilder()
        .sql("select dim1,dim4,d1,f1 from TABLE(APPEND('foo','numfoo')) u")
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
        .expectedLogicalPlan(
            ""
                + "LogicalProject(exprs=[[$1, $8, $11, $13]])\n"
                + "  LogicalTableScan(table=[[APPEND]])\n"
        )
        .run();
  }

  @Test
  public void testAppendSameTableMultipleTimes()
  {
    testBuilder()
        .sql("select dim1,dim4,d1,f1 from TABLE(APPEND('foo','numfoo','foo')) u where dim1='2'")
        .expectedResults(
            ImmutableList.of(
                new Object[] {"2", null, null, null},
                new Object[] {"2", null, null, null},
                new Object[] {"2", "a", 0.0D, 0.0F}
            )
        )
        .run();
  }

  @Test
  public void testAppendtSingleTableIsInvalid()
  {
    try {
      testBuilder()
          .sql("select dim1 from TABLE(APPEND('foo')) u")
          .run();
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(
          e,
          invalidSqlIs("No match found for function signature APPEND(<CHARACTER>) (line [1], column [24])")
      );
    }
  }

  @Test
  public void testAppendNoTableIsInvalid()
  {
    try {
      testBuilder()
          .sql("select dim1 from TABLE(APPEND()) u")
          .run();
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(
          e,
          invalidSqlIs("No match found for function signature APPEND() (line [1], column [24])")
      );
    }
  }

  @Test
  public void testAppendtSingleTableIsInvalidArg()
  {
    try {
      testBuilder()
          .sql("select dim1 from TABLE(APPEND('foo',111)) u")
          .run();
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(
          e,
          invalidSqlIs(
              "All arguments to APPEND should be literal strings. Argument #1 is not string (line [1], column [37])"
          )
      );
    }
  }

  @Test
  public void testAppendtNonExistentTable()
  {
    try {
      testBuilder()
          .sql("select dim1 from TABLE(APPEND('foo','nonexistent')) u")
          .run();
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(
          e,
          invalidSqlIs("Table [nonexistent] not found (line [1], column [37])")
      );
    }
  }

  @Test
  public void testAppendCompatibleColumns()
  {
    testBuilder()
        .sql("select dim3 from TABLE(APPEND('foo','foo2')) u")
        .expectedResults(
            ImmutableList.of(
                new Object[] {"11"},
                new Object[] {"12"},
                new Object[] {"10"},
                new Object[] {"[\"a\",\"b\"]"},
                new Object[] {"[\"b\",\"c\"]"},
                new Object[] {"d"},
                new Object[] {""},
                new Object[] {null},
                new Object[] {null}
            )
        )
        .run();
  }

  @Test
  public void testAppendtIncompatibleColumns()
  {
    try {
      testBuilder()
          .sql("select dim1 from TABLE(APPEND('foo','foo2')) u")
          .run();
      Assert.fail("query execution should fail");
    }
    catch (DruidException e) {
      MatcherAssert.assertThat(
          e,
          DruidExceptionMatcher.invalidInput().expectMessageIs(
              "Can't create TABLE(APPEND()).\n"
                  + "Conflicting types for column [dim3]:\n"
                  + " - existing type [STRING]\n"
                  + " - new type [LONG] from table [[druid, foo2]]"
          )
      );
    }
  }
}
