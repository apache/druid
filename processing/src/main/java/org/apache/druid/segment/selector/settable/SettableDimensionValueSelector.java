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

package org.apache.druid.segment.selector.settable;

import com.google.common.base.Predicate;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Settable implementation of {@link DimensionSelector}.
 */
public class SettableDimensionValueSelector implements SettableColumnValueSelector, DimensionSelector
{
  @Nullable
  private DimensionSelector keptSelector;
  private final ArrayBasedIndexedInts keptRow = new ArrayBasedIndexedInts();

  @Override
  public void setValueFrom(ColumnValueSelector selector)
  {
    DimensionSelector dimensionSelector = (DimensionSelector) selector;
    keptSelector = dimensionSelector;
    IndexedInts row = dimensionSelector.getRow();
    int rowSize = row.size();
    keptRow.ensureSize(rowSize);
    for (int i = 0; i < rowSize; i++) {
      keptRow.setValue(i, row.get(i));
    }
    keptRow.setSize(rowSize);
  }

  @Override
  public IndexedInts getRow()
  {
    return keptRow;
  }

  @Override
  public ValueMatcher makeValueMatcher(@Nullable String value)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public ValueMatcher makeValueMatcher(Predicate<String> predicate)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int getValueCardinality()
  {
    throw new UnsupportedOperationException();
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    return Objects.requireNonNull(keptSelector).lookupName(id);
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return Objects.requireNonNull(keptSelector).nameLookupPossibleInAdvance();
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return Objects.requireNonNull(keptSelector).idLookup();
  }

  @Nullable
  @Override
  public Object getObject()
  {
    return defaultGetObject();
  }

  @Override
  public Class classOfObject()
  {
    return Object.class;
  }
}
