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

import com.google.common.base.Predicate;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * {@link DimensionSelector} for {@link Map} type {@link MapVirtualColumn}. This dimensionSelector only supports
 * {@link #getObject()} currently.
 */
final class MapTypeMapVirtualColumnDimensionSelector extends MapVirtualColumnDimensionSelector
{
  MapTypeMapVirtualColumnDimensionSelector(
      DimensionSelector keySelector,
      DimensionSelector valueSelector
  )
  {
    super(keySelector, valueSelector);
  }

  @Override
  public IndexedInts getRow()
  {
    throw new UnsupportedOperationException("Map column doesn't support getRow()");
  }

  @Override
  public ValueMatcher makeValueMatcher(@Nullable String value)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        // Map column doesn't match with any string
        return false;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    };
  }

  @Override
  public ValueMatcher makeValueMatcher(Predicate<String> predicate)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return false;
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {

      }
    };
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
    throw new UnsupportedOperationException("Map column doesn't support lookupName()");
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return false;
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    throw new UnsupportedOperationException("Map column doesn't support idLookup()");
  }

  @Override
  public Object getObject()
  {
    final DimensionSelector keySelector = getKeySelector();
    final DimensionSelector valueSelector = getValueSelector();

    final IndexedInts keyIndices = keySelector.getRow();
    final IndexedInts valueIndices = valueSelector.getRow();

    final int limit = Math.min(keyIndices.size(), valueIndices.size());
    return IntStream
        .range(0, limit)
        .boxed()
        .collect(
            Collectors.toMap(
                i -> keySelector.lookupName(keyIndices.get(i)),
                i -> valueSelector.lookupName(valueIndices.get(i))
            )
        );
  }

  @Override
  public Class classOfObject()
  {
    return Map.class;
  }
}
