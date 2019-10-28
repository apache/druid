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

package org.apache.druid.query;

import com.google.common.collect.Iterables;
import junit.framework.Assert;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.context.ResponseContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class UnionQueryRunnerTest
{
  @Test
  public void testUnionQueryRunner()
  {
    AtomicBoolean ds1 = new AtomicBoolean(false);
    AtomicBoolean ds2 = new AtomicBoolean(false);
    QueryRunner baseRunner = new QueryRunner()
    {
      @Override
      public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
      {
        // verify that table datasource is passed to baseQueryRunner
        Assert.assertTrue(queryPlus.getQuery().getDataSource() instanceof TableDataSource);
        String dsName = Iterables.getOnlyElement(queryPlus.getQuery().getDataSource().getNames());
        if ("ds1".equals(dsName)) {
          ds1.compareAndSet(false, true);
          return Sequences.simple(Arrays.asList(1, 2, 3));
        } else if ("ds2".equals(dsName)) {
          ds2.compareAndSet(false, true);
          return Sequences.simple(Arrays.asList(4, 5, 6));
        } else {
          throw new AssertionError("Unexpected DataSource");
        }
      }
    };
    UnionQueryRunner runner = new UnionQueryRunner(baseRunner);
    // Make a dummy query with Union datasource
    Query q = Druids.newTimeseriesQueryBuilder()
                    .dataSource(
                        new UnionDataSource(
                            Arrays.asList(
                                new TableDataSource("ds1"),
                                new TableDataSource("ds2")
                            )
                        )
                    )
                    .intervals("2014-01-01T00:00:00Z/2015-01-01T00:00:00Z")
                    .aggregators(QueryRunnerTestHelper.COMMON_DOUBLE_AGGREGATORS)
                    .build();
    ResponseContext responseContext = ResponseContext.createEmpty();
    Sequence<?> result = runner.run(QueryPlus.wrap(q), responseContext);
    List res = result.toList();
    Assert.assertEquals(Arrays.asList(1, 2, 3, 4, 5, 6), res);
    Assert.assertEquals(true, ds1.get());
    Assert.assertEquals(true, ds2.get());
  }

}
