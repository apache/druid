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

package org.apache.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.common.guava.SettableSupplier;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.groupby.epinephelinae.RowBasedGrouperHelper;

import java.util.Objects;
import java.util.function.Function;

public class DimFilterHavingSpec implements HavingSpec
{
  private static final boolean DEFAULT_FINALIZE = true;

  private final DimFilter dimFilter;
  private final boolean finalize;
  private final SettableSupplier<ResultRow> rowSupplier = new SettableSupplier<>();

  private Int2ObjectMap<Function<Object, Object>> finalizers = new Int2ObjectArrayMap<>();
  private ValueMatcher matcher = null;
  private int evalCount;

  @JsonCreator
  public DimFilterHavingSpec(
      @JsonProperty("filter") final DimFilter dimFilter,
      @JsonProperty("finalize") final Boolean finalize
  )
  {
    this.dimFilter = Preconditions.checkNotNull(dimFilter, "filter");
    this.finalize = finalize == null ? DEFAULT_FINALIZE : finalize;
  }

  @JsonProperty("filter")
  public DimFilter getDimFilter()
  {
    return dimFilter;
  }

  @JsonProperty
  public boolean isFinalize()
  {
    return finalize;
  }

  @Override
  public void setQuery(GroupByQuery query)
  {
    this.finalizers = new Int2ObjectArrayMap<>(query.getAggregatorSpecs().size());

    for (AggregatorFactory factory : query.getAggregatorSpecs()) {
      final int i = query.getResultRowPositionLookup().getInt(factory.getName());
      this.finalizers.put(i, factory::finalizeComputation);
    }

    this.matcher = dimFilter.toFilter().makeMatcher(
        RowBasedGrouperHelper.createResultRowBasedColumnSelectorFactory(
            query,
            rowSupplier
        )
    );
  }

  @Override
  public boolean eval(final ResultRow row)
  {
    int oldEvalCount = evalCount;
    evalCount++;

    if (finalize && !finalizers.isEmpty()) {
      // Create finalized copy.
      final ResultRow finalizedCopy = row.copy();

      for (Int2ObjectMap.Entry<Function<Object, Object>> entry : finalizers.int2ObjectEntrySet()) {
        finalizedCopy.set(entry.getIntKey(), entry.getValue().apply(row.get(entry.getIntKey())));
      }

      rowSupplier.set(finalizedCopy);
    } else {
      rowSupplier.set(row);
    }

    final boolean retVal = matcher.matches();

    if (evalCount != oldEvalCount + 1) {
      // Oops, someone was using this from two different threads, bad caller.
      throw new IllegalStateException("concurrent 'eval' calls not permitted!");
    }

    return retVal;
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final DimFilterHavingSpec that = (DimFilterHavingSpec) o;
    return finalize == that.finalize &&
           Objects.equals(dimFilter, that.dimFilter);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dimFilter, finalize);
  }

  @Override
  public String toString()
  {
    return "DimFilterHavingSpec{" +
           "dimFilter=" + dimFilter +
           ", finalize=" + finalize +
           '}';
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(HavingSpecUtil.CACHE_TYPE_ID_DIM_FILTER)
        .appendCacheable(dimFilter)
        .appendByte((byte) (isFinalize() ? 1 : 0))
        .build();
  }
}
