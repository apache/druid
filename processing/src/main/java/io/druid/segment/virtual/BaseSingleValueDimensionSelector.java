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

package io.druid.segment.virtual;

import com.google.common.base.Predicate;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.CalledFromHotLoop;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.DimensionSelector;
import io.druid.segment.IdLookup;
import io.druid.segment.SingleValueDimensionSelector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ZeroIndexedInts;

import javax.annotation.Nullable;
import java.util.Objects;

public abstract class BaseSingleValueDimensionSelector implements SingleValueDimensionSelector
{
  @CalledFromHotLoop
  protected abstract String getValue();

  @Override
  public IndexedInts getRow()
  {
    return ZeroIndexedInts.instance();
  }

  @Override
  public int getRowValue()
  {
    return 0;
  }

  @Override
  public int getValueCardinality()
  {
    return DimensionSelector.CARDINALITY_UNKNOWN;
  }

  @Override
  public String lookupName(int id)
  {
    return getValue();
  }

  @Override
  public ValueMatcher makeValueMatcher(final String value)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return Objects.equals(getValue(), value);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", BaseSingleValueDimensionSelector.this);
      }
    };
  }

  @Override
  public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return predicate.apply(getValue());
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", BaseSingleValueDimensionSelector.this);
        inspector.visit("predicate", predicate);
      }
    };
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
    return null;
  }
}
