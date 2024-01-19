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

import com.google.common.base.Preconditions;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.DruidObjectPredicate;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.SingleIndexedInt;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A special {@link DimensionSelector} for projected time columns
 * - it assumes time values are scanned once and values are grouped together
 *   (i.e. we never revisit a timestamp we have seen before, unless it is the same as the last accessed one)
 * - it also applies and caches extraction function values at the {@link DimensionSelector} level to speed things up
 */
public class SingleScanTimeDimensionSelector implements DimensionSelector
{
  private final ExtractionFn extractionFn;
  private final BaseLongColumnValueSelector selector;
  private final boolean descending;

  private final List<String> timeValues = new ArrayList<>();
  private final SingleIndexedInt row = new SingleIndexedInt();
  private long currentTimestamp = Long.MIN_VALUE;
  private int index = -1;

  @Nullable
  private String currentValue = null;

  public SingleScanTimeDimensionSelector(
      BaseLongColumnValueSelector selector,
      @Nullable ExtractionFn extractionFn,
      boolean descending
  )
  {
    Preconditions.checkNotNull(extractionFn, "time dimension must provide an extraction function");
    this.extractionFn = extractionFn;
    this.selector = selector;
    this.descending = descending;
  }

  @Override
  public IndexedInts getRow()
  {
    row.setValue(getDimensionValueIndex());
    return row;
  }

  @Override
  public ValueMatcher makeValueMatcher(final @Nullable String value)
  {
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        final String rowVal = lookupName(getDimensionValueIndex());
        return (includeUnknown && rowVal == null) || Objects.equals(rowVal, value);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", SingleScanTimeDimensionSelector.this);
      }
    };
  }

  @Override
  public ValueMatcher makeValueMatcher(final DruidPredicateFactory predicateFactory)
  {
    final DruidObjectPredicate<String> predicate = predicateFactory.makeStringPredicate();
    return new ValueMatcher()
    {
      @Override
      public boolean matches(boolean includeUnknown)
      {
        return predicate.apply(lookupName(getDimensionValueIndex())).matches(includeUnknown);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        inspector.visit("selector", SingleScanTimeDimensionSelector.this);
        inspector.visit("predicate", predicateFactory);
      }
    };
  }

  private int getDimensionValueIndex()
  {
    // if this the first timestamp, apply and cache extraction function result
    final long timestamp = selector.getLong();
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

  @Nullable
  @Override
  public String getObject()
  {
    return currentValue;
  }

  @Override
  public Class<String> classOfObject()
  {
    return String.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("selector", selector);
    inspector.visit("extractionFn", extractionFn);
    inspector.visit("descending", descending);
  }
}
