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

import org.apache.druid.error.DruidException;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;
import org.apache.druid.segment.filter.ValueMatchers;
import org.apache.druid.segment.historical.HistoricalDimensionSelector;
import org.apache.druid.utils.CollectionUtils;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Objects;

public class ConstantMultiValueDimensionSelector implements HistoricalDimensionSelector
{
  private final List<String> values;
  private final RangeIndexedInts row;

  public ConstantMultiValueDimensionSelector(List<String> values)
  {
    if (CollectionUtils.isNullOrEmpty(values)) {
      throw DruidException.defensive("Use DimensionSelector.constant(null)");
    }

    this.values = values;
    this.row = new RangeIndexedInts();
    row.setSize(values.size());
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("values", values);
    inspector.visit("row", row);
  }

  @Nullable
  @Override
  public Object getObject()
  {
    return defaultGetObject();
  }

  @Override
  public Class<?> classOfObject()
  {
    return Object.class;
  }

  @Override
  public int getValueCardinality()
  {
    return CARDINALITY_UNKNOWN;
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    return values.get(id);
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
    return null;
  }

  @Override
  public IndexedInts getRow()
  {
    return row;
  }

  @Override
  public ValueMatcher makeValueMatcher(@Nullable String value)
  {
    return values.stream().anyMatch(v -> Objects.equals(value, v)) ? ValueMatchers.allTrue() : ValueMatchers.allFalse();
  }

  @Override
  public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
  {
    final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();
    return values.stream().anyMatch(x -> predicate.apply(x).matches(false)) ? ValueMatchers.allTrue() : ValueMatchers.allFalse();
  }

  @Override
  public IndexedInts getRow(int offset)
  {
    return row;
  }
}
