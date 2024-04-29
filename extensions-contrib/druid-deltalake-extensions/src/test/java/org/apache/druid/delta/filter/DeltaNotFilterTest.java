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

package org.apache.druid.delta.filter;

import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.LongType;
import io.delta.kernel.types.StringType;
import io.delta.kernel.types.StructField;
import io.delta.kernel.types.StructType;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.common.StringUtils;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class DeltaNotFilterTest
{
  private static final StructType SCHEMA = new StructType()
      .add(new StructField("name", StringType.STRING, true))
      .add(new StructField("age", LongType.LONG, false))
      .add(new StructField("bar", StringType.STRING, true));

  @Test
  public void testNotFilterWithEqualsExpression()
  {
    DeltaEqualsFilter equalsFilter = new DeltaEqualsFilter(
        "name",
        "Employee1"
    );
    DeltaNotFilter notFilter = new DeltaNotFilter(equalsFilter);

    Predicate predicate = notFilter.getFilterPredicate(SCHEMA);

    Assert.assertEquals(predicate.getName(), "NOT");
    Assert.assertEquals(1, predicate.getChildren().size());
  }

  @Test
  public void testNotFilterWithAndExpression()
  {
    DeltaAndFilter andFilter = new DeltaAndFilter(
        Arrays.asList(
          new DeltaEqualsFilter(
              "name",
              "Employee1"
          ),
          new DeltaEqualsFilter(
              "name",
              "Employee2"
          )
        )
    );
    DeltaNotFilter notFilter = new DeltaNotFilter(andFilter);

    Predicate predicate = notFilter.getFilterPredicate(SCHEMA);

    Assert.assertEquals(predicate.getName(), "NOT");
    Assert.assertEquals(1, predicate.getChildren().size());
  }

  @Test
  public void testNotFilterWithInvalidColumn()
  {
    DeltaEqualsFilter equalsFilter = new DeltaEqualsFilter(
        "name2",
        "Employee1"
    );
    DeltaNotFilter notFilter = new DeltaNotFilter(equalsFilter);

    MatcherAssert.assertThat(
        Assert.assertThrows(DruidException.class, () -> notFilter.getFilterPredicate(SCHEMA)),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            StringUtils.format("column[name2] doesn't exist in schema[%s]", SCHEMA)
        )
    );
  }

  @Test
  public void testNotFilterWithNoFilterPredicates()
  {
    MatcherAssert.assertThat(
        Assert.assertThrows(
            DruidException.class,
            () -> new DeltaNotFilter(null)
        ),
        DruidExceptionMatcher.invalidInput().expectMessageIs(
            "Delta not filter requiers 1 filter predicate and must be non-empty. None provided."
        )
    );
  }
}
