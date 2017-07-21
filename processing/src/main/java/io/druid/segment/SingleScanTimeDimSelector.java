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

package io.druid.segment;

import com.google.common.base.Predicate;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.SingleIndexedInt;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class SingleScanTimeDimSelector implements SingleValueDimensionSelector
{
  private final ExtractionFn extractionFn;
  private final LongColumnSelector selector;
  private final boolean descending;

  private final List<String> timeValues = new ArrayList<>();
  private String currentValue = null;
  private long currentTimestamp = Long.MIN_VALUE;
  private int index = -1;


  // Use a special DimSelector for projected time columns
  // - it assumes time values are scanned once and values are grouped together
  //   (i.e. we never revisit a timestamp we have seen before, unless it is the same as the last accessed one)
  // - it also applies and caches extraction function values at the DimSelector level to speed things up
  public SingleScanTimeDimSelector(LongColumnSelector selector, ExtractionFn extractionFn, boolean descending)
  {
    if (extractionFn == null) {
      throw new UnsupportedOperationException("time dimension must provide an extraction function");
    }

    this.extractionFn = extractionFn;
    this.selector = selector;
    this.descending = descending;
  }

  @Override
  public IndexedInts getRow()
  {
    return new SingleIndexedInt(getDimensionValueIndex());
  }

  @Override
  public int getRowValue()
  {
    return getDimensionValueIndex();
  }

  @Override
  public ValueMatcher makeValueMatcher(final String value)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches()
      {
        return Objects.equals(lookupName(getDimensionValueIndex()), value);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", SingleScanTimeDimSelector.this);
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
        return predicate.apply(lookupName(getDimensionValueIndex()));
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", SingleScanTimeDimSelector.this);
        inspector.visit("predicate", predicate);
      }
    };
  }

  private int getDimensionValueIndex()
  {
    // if this the first timestamp, apply and cache extraction function result
    final long timestamp = selector.get();
    if (index < 0) {
      currentTimestamp = timestamp;
      currentValue = extractionFn.apply(timestamp);
      ++index;
      timeValues.add(currentValue);
      // if this is a new timestamp, apply and cache extraction function result
      // since timestamps are assumed grouped and scanned once, we only need to
      // check if the current timestamp is different than the current timestamp.
      //
      // If this new timestamp is mapped to the same value by the extraction function,
      // we can also avoid creating a dimension value and corresponding index
      // and use the current one
    } else if (timestamp != currentTimestamp) {
      if (descending ? timestamp > currentTimestamp : timestamp < currentTimestamp) {
        // re-using this selector for multiple scans would cause the same rows to return different IDs
        // we might want to re-visit if we ever need to do multiple scans with this dimension selector
        throw new IllegalStateException("cannot re-use time dimension selector for multiple scans");
      }
      currentTimestamp = timestamp;
      final String value = extractionFn.apply(timestamp);
      if (!Objects.equals(value, currentValue)) {
        currentValue = value;
        ++index;
        timeValues.add(currentValue);
      }
      // Note: this could be further optimized by checking if the new value is one we have
      // previously seen, but would require keeping track of both the current and the maximum index
    }
    // otherwise, if the current timestamp is the same as the previous timestamp,
    // keep using the same dimension value index

    return index;
  }

  @Override
  public int getValueCardinality()
  {
    return Integer.MAX_VALUE;
  }

  @Override
  public String lookupName(int id)
  {
    if (id == index) {
      return currentValue;
    } else {
      return timeValues.get(id);
    }
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

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    inspector.visit("extractionFn", extractionFn);
    inspector.visit("descending", descending);
  }
}
