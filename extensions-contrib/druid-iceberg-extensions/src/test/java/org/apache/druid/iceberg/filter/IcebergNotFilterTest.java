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

package org.apache.druid.iceberg.filter;

import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class IcebergNotFilterTest
{
  @Test
  public void testFilter()
  {
    IcebergNotFilter testFilter = new IcebergNotFilter(new IcebergEqualsFilter("column1", "value1"));
    Expression expectedExpression = Expressions.not(Expressions.equal("column1", "value1"));
    Assert.assertEquals(expectedExpression.toString(), testFilter.getFilterExpression().toString());
  }

  @Test
  public void testNestedFilters()
  {
    String column1 = "column1";
    String column2 = "column2";

    IcebergNotFilter filterNotAnd = new IcebergNotFilter(
        new IcebergAndFilter(Arrays.asList(
            new IcebergEqualsFilter(
                column1,
                "value1"
            ),
            new IcebergEqualsFilter(
                column2,
                "value2"
            )
        ))
    );
    Expression expressionNotAnd = Expressions.not(Expressions.and(
        Expressions.equal(column1, "value1"),
        Expressions.equal(column2, "value2")
    ));

    IcebergNotFilter filterNotOr = new IcebergNotFilter(
        new IcebergOrFilter(Arrays.asList(
            new IcebergEqualsFilter(
                column1,
                "value1"
            ),
            new IcebergEqualsFilter(
                column2,
                "value2"
            )
        ))
    );
    Expression expressionNotOr = Expressions.not(Expressions.or(
        Expressions.equal(column1, "value1"),
        Expressions.equal(column2, "value2")
    ));

    Assert.assertEquals(expressionNotAnd.toString(), filterNotAnd.getFilterExpression().toString());
    Assert.assertEquals(expressionNotOr.toString(), filterNotOr.getFilterExpression().toString());
  }
}
