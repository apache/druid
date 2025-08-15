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

package org.apache.druid.segment.projections;

import org.apache.druid.query.filter.EqualityFilter;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.filter.AndFilter;
import org.apache.druid.segment.filter.IsBooleanFilter;
import org.apache.druid.segment.filter.OrFilter;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class ProjectionFilterMatchTest
{
  @Test
  void testRewriteFilter()
  {
    Filter xeqfoo = new EqualityFilter("x", ColumnType.STRING, "foo", null);
    Filter xeqfoo2 = new EqualityFilter("x", ColumnType.STRING, "foo", null);
    Filter xeqbar = new EqualityFilter("x", ColumnType.STRING, "bar", null);
    Filter yeqbar = new EqualityFilter("y", ColumnType.STRING, "bar", null);
    Filter zeq123 = new EqualityFilter("z", ColumnType.LONG, 123L, null);

    Filter queryFilter = xeqfoo2;
    Assertions.assertInstanceOf(
        ProjectionFilterMatch.class,
        ProjectionFilterMatch.rewriteFilter(xeqfoo, queryFilter)
    );

    queryFilter = yeqbar;
    Assertions.assertNull(ProjectionFilterMatch.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(xeqfoo, yeqbar));
    Assertions.assertEquals(
        yeqbar,
        ProjectionFilterMatch.rewriteFilter(xeqfoo, queryFilter)
    );

    queryFilter = new AndFilter(List.of(new OrFilter(List.of(xeqfoo, xeqbar)), yeqbar));
    Assertions.assertNull(ProjectionFilterMatch.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(new IsBooleanFilter(xeqfoo, true), yeqbar));
    Assertions.assertEquals(yeqbar, ProjectionFilterMatch.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(new IsBooleanFilter(xeqfoo, false), yeqbar));
    Assertions.assertNull(ProjectionFilterMatch.rewriteFilter(xeqfoo, queryFilter));

    queryFilter = new AndFilter(List.of(new AndFilter(List.of(xeqfoo, yeqbar)), zeq123));
    Assertions.assertEquals(
        new AndFilter(List.of(yeqbar, zeq123)),
        ProjectionFilterMatch.rewriteFilter(xeqfoo, queryFilter)
    );

    queryFilter = new AndFilter(
        List.of(
            new EqualityFilter("a", ColumnType.STRING, "foo", null),
            new EqualityFilter("b", ColumnType.STRING, "bar", null),
            new EqualityFilter("c", ColumnType.STRING, "baz", null)
        )
    );
    Assertions.assertEquals(
        new EqualityFilter("b", ColumnType.STRING, "bar", null),
        ProjectionFilterMatch.rewriteFilter(
            new AndFilter(
                List.of(
                    new EqualityFilter("a", ColumnType.STRING, "foo", null),
                    new EqualityFilter("c", ColumnType.STRING, "baz", null)
                )
            ),
            queryFilter
        )
    );
  }

  @Test
  void testRewriteFilter_andFilterNotMatch()
  {
    Filter xeqfoo = new EqualityFilter("x", ColumnType.STRING, "foo", null);
    Filter metricLike = new EqualityFilter("metric", ColumnType.STRING, "metric1", null);
    Filter envOrUser = new OrFilter(List.of(
        new EqualityFilter("env", ColumnType.STRING, "dev", null),
        new EqualityFilter("user", ColumnType.STRING, "user-a", null)
    ));
    Filter envOrUser2 = new OrFilter(List.of(
        new EqualityFilter("prod", ColumnType.STRING, "dev", null),
        new EqualityFilter("another-user", ColumnType.STRING, "user-a", null)
    ));

    Filter queryFilter = new AndFilter(List.of(xeqfoo, envOrUser, metricLike));
    Filter projectionFilter = new AndFilter(List.of(envOrUser2, metricLike));

    Assertions.assertNull(ProjectionFilterMatch.rewriteFilter(projectionFilter, queryFilter));
  }

  @Test
  void testRewriteFilter_andFilterMatch()
  {
    Filter xeqfoo = new EqualityFilter("x", ColumnType.STRING, "foo", null);
    Filter metricLike = new EqualityFilter("metric", ColumnType.STRING, "metric1", null);
    Filter envOrUser = new OrFilter(List.of(
        new EqualityFilter("env", ColumnType.STRING, "dev", null),
        new EqualityFilter("user", ColumnType.STRING, "user-a", null)
    ));

    Filter queryFilter = new AndFilter(List.of(xeqfoo, envOrUser, metricLike));
    Filter projectionFilter = new AndFilter(List.of(envOrUser, metricLike));

    Assertions.assertEquals(xeqfoo, ProjectionFilterMatch.rewriteFilter(projectionFilter, queryFilter));
  }
}
