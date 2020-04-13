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

package org.apache.druid.segment.join.table;

import com.google.common.base.Predicate;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.SingleIndexedInt;

import javax.annotation.Nullable;
import java.util.function.IntSupplier;

public class IndexedTableDimensionSelector implements DimensionSelector
{
  private final IndexedTable table;
  private final IntSupplier currentRow;
  @Nullable
  private final ExtractionFn extractionFn;
  private final IndexedTable.Reader columnReader;
  private final SingleIndexedInt currentIndexedInts;

  IndexedTableDimensionSelector(
      IndexedTable table,
      IntSupplier currentRow,
      int columnNumber,
      @Nullable ExtractionFn extractionFn
  )
  {
    this.table = table;
    this.currentRow = currentRow;
    this.extractionFn = extractionFn;
    this.columnReader = table.columnReader(columnNumber);
    this.currentIndexedInts = new SingleIndexedInt();
  }

  @Override
  public IndexedInts getRow()
  {
    final int rowNum = currentRow.getAsInt();

    if (rowNum == -1) {
      // Null value.
      currentIndexedInts.setValue(table.numRows());
    } else {
      currentIndexedInts.setValue(rowNum);
    }

    return currentIndexedInts;
  }

  @Override
  public ValueMatcher makeValueMatcher(@Nullable String value)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
  }

  @Override
  public ValueMatcher makeValueMatcher(Predicate<String> predicate)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicate);
  }

  @Override
  public int getValueCardinality()
  {
    return computeDimensionSelectorCardinality(table);
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    final String value;

    if (id == table.numRows()) {
      value = null;
    } else {
      value = DimensionHandlerUtils.convertObjectToString(columnReader.read(id));
    }

    if (extractionFn == null) {
      return value;
    } else {
      return extractionFn.apply(value);
    }
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

  @Nullable
  @Override
  public Object getObject()
  {
    return lookupName(currentRow.getAsInt());
  }

  @Override
  public Class<?> classOfObject()
  {
    return String.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    inspector.visit("table", table);
    inspector.visit("extractionFn", extractionFn);
  }

  /**
   * Returns the value that {@link #getValueCardinality()} would return for a particular {@link IndexedTable}.
   *
   * The value will be one higher than {@link IndexedTable#numRows()}, to account for the possibility of phantom nulls.
   *
   * @throws IllegalArgumentException if the table's row count is {@link Integer#MAX_VALUE}
   */
  static int computeDimensionSelectorCardinality(final IndexedTable table)
  {
    if (table.numRows() == Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Table is too large");
    }

    return table.numRows() + 1;
  }
}
