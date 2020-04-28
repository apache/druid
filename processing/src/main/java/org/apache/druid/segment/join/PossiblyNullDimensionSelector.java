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

package org.apache.druid.segment.join;

import com.google.common.base.Predicate;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.BooleanSupplier;

public class PossiblyNullDimensionSelector extends AbstractDimensionSelector implements IdLookup
{
  private final DimensionSelector baseSelector;
  private final BooleanSupplier beNull;
  private final NullAdjustedIndexedInts nullAdjustedRow;

  // 1 for selectors that have dictionaries (cardinality >= 0), but don't contain null.
  private final int nullAdjustment;

  public PossiblyNullDimensionSelector(DimensionSelector baseSelector, BooleanSupplier beNull)
  {
    this.baseSelector = baseSelector;
    this.beNull = beNull;

    if (baseSelector.nameLookupPossibleInAdvance() &&
        (baseSelector.getValueCardinality() == 0 || (baseSelector.getValueCardinality() > 0
                                                     && baseSelector.lookupName(0) != null))) {
      this.nullAdjustment = 1;
    } else {
      this.nullAdjustment = 0;
    }

    this.nullAdjustedRow = new NullAdjustedIndexedInts(nullAdjustment);
  }

  @Override
  @Nonnull
  public IndexedInts getRow()
  {
    if (beNull.getAsBoolean()) {
      // This is the reason we have all the nullAdjustment business. We need to return a null when asked to.
      return ZeroIndexedInts.instance();
    } else {
      nullAdjustedRow.set(baseSelector.getRow());
      return nullAdjustedRow;
    }
  }

  @Override
  @Nonnull
  public ValueMatcher makeValueMatcher(@Nullable final String value)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
  }

  @Override
  @Nonnull
  public ValueMatcher makeValueMatcher(final Predicate<String> predicate)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
  }

  @Override
  public int getValueCardinality()
  {
    return baseSelector.getValueCardinality() + nullAdjustment;
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    final int cardinality = getValueCardinality();

    if (cardinality == CARDINALITY_UNKNOWN) {
      // CARDINALITY_UNKNOWN means lookupName is only being called in the context of a single row,
      // so it's safe to look at "beNull" here.
      if (beNull.getAsBoolean()) {
        assert id == 0;
        return null;
      } else {
        return baseSelector.lookupName(id - nullAdjustment);
      }
    } else {
      assert cardinality > 0;

      if (id == 0) {
        // id 0 is always null for this selector impl.
        return null;
      } else {
        return baseSelector.lookupName(id - nullAdjustment);
      }
    }
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return baseSelector.nameLookupPossibleInAdvance();
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return baseSelector.idLookup() != null ? this : null;
  }

  @Override
  public int lookupId(@Nullable String name)
  {
    if (name == null) {
      // id 0 is always null for this selector impl.
      return 0;
    } else {
      IdLookup idLookup = baseSelector.idLookup();
      // idLookup is null here because callers are expected to check this condition before calling lookupId
      assert idLookup != null;
      return idLookup.lookupId(name) + nullAdjustment;
    }
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("beNull", beNull);
    baseSelector.inspectRuntimeShape(inspector);
  }

  @Override
  public Class<?> classOfObject()
  {
    return baseSelector.classOfObject();
  }

  private static class NullAdjustedIndexedInts implements IndexedInts
  {
    private final int nullAdjustment;
    private IndexedInts ints = null;

    public NullAdjustedIndexedInts(int nullAdjustment)
    {
      this.nullAdjustment = nullAdjustment;
    }

    public void set(IndexedInts ints)
    {
      this.ints = ints;
    }

    @Override
    public int size()
    {
      return ints.size();
    }

    @Override
    public int get(int index)
    {
      return ints.get(index) + nullAdjustment;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      inspector.visit("ints", ints);
    }
  }
}
