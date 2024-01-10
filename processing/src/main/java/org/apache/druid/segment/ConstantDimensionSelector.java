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

package org.apache.druid.segment;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.DruidPredicateMatch;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;
import org.apache.druid.segment.filter.ValueMatchers;
import org.apache.druid.segment.historical.SingleValueHistoricalDimensionSelector;

import javax.annotation.Nullable;
import java.util.Objects;

public class ConstantDimensionSelector implements SingleValueHistoricalDimensionSelector, IdLookup
{
  private final String value;

  public ConstantDimensionSelector(final String value)
  {
    if (NullHandling.isNullOrEquivalent(value)) {
      // There's an optimized implementation for nulls that callers should use instead.
      throw new IllegalArgumentException("Use DimensionSelector.constant(null)");
    }

    this.value = value;
  }

  @Override
  public IndexedInts getRow()
  {
    return ZeroIndexedInts.instance();
  }

  @Override
  public int getRowValue(int offset)
  {
    return 0;
  }

  @Override
  public IndexedInts getRow(int offset)
  {
    return getRow();
  }

  @Override
  public ValueMatcher makeValueMatcher(String matchValue)
  {
    return Objects.equals(value, matchValue) ? ValueMatchers.allTrue() : ValueMatchers.allFalse();
  }

  @Override
  public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
  {
    final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();
    final DruidPredicateMatch match = predicate.apply(value);
    if (match == DruidPredicateMatch.TRUE) {
      return ValueMatchers.allTrue();
    }
    if (match == DruidPredicateMatch.UNKNOWN) {
      return ValueMatchers.allUnknown();
    }
    return ValueMatchers.allFalse();
  }

  @Override
  public int getValueCardinality()
  {
    return 1;
  }

  @Override
  public String lookupName(int id)
  {
    assert id == 0 : "id = " + id;
    return value;
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return true;
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return this;
  }

  @Override
  public int lookupId(String name)
  {
    return value.equals(name) ? 0 : -1;
  }

  @Nullable
  @Override
  public Object getObject()
  {
    return value;
  }

  @Override
  public Class classOfObject()
  {
    return String.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("value", value);
  }
}
