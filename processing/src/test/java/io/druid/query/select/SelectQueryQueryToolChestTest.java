/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.select;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.CacheStrategy;
import io.druid.query.Druids;
import io.druid.query.QueryRunnerTestHelper;
import io.druid.query.Result;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

public class SelectQueryQueryToolChestTest
{
  private static final Supplier<SelectQueryConfig> configSupplier = Suppliers.ofInstance(new SelectQueryConfig(true));

  private static final SelectQueryQueryToolChest toolChest = new SelectQueryQueryToolChest(
      new DefaultObjectMapper(),
      QueryRunnerTestHelper.NoopIntervalChunkingQueryRunnerDecorator(),
      configSupplier
  );

  @Test
  public void testComputeCacheKeyWithDifferentSortOrer()
  {
    final SelectQuery query1 = Druids.newSelectQueryBuilder()
                                     .dataSource("dummy")
                                     .dimensions(Collections.singletonList("testDim"))
                                     .intervals(SelectQueryRunnerTest.I_0112_0114)
                                     .granularity(QueryRunnerTestHelper.allGran)
                                     .pagingSpec(PagingSpec.newSpec(3))
                                     .descending(false)
                                     .build();

    final SelectQuery query2 = Druids.newSelectQueryBuilder()
                                     .dataSource("dummy")
                                     .dimensions(Collections.singletonList("testDim"))
                                     .intervals(SelectQueryRunnerTest.I_0112_0114)
                                     .granularity(QueryRunnerTestHelper.allGran)
                                     .pagingSpec(PagingSpec.newSpec(3))
                                     .descending(true)
                                     .build();

    final CacheStrategy<Result<SelectResultValue>, Object, SelectQuery> strategy1 = toolChest.getCacheStrategy(query1);
    Assert.assertNotNull(strategy1);
    final CacheStrategy<Result<SelectResultValue>, Object, SelectQuery> strategy2 = toolChest.getCacheStrategy(query2);
    Assert.assertNotNull(strategy2);

    Assert.assertFalse(Arrays.equals(strategy1.computeCacheKey(query1), strategy2.computeCacheKey(query2)));
  }
}
