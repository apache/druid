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

package org.apache.druid.segment.shim;

import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.SingleIndexedInt;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;

/**
 * Shim from {@link SingleValueDimensionVectorSelector} to {@link ColumnValueSelector} for a {@link ShimCursor}.
 */
public class ShimSingleValueDimensionSelector implements DimensionSelector
{
  private final ShimCursor cursor;
  private final ReadableVectorInspector vectorInspector;
  private final SingleValueDimensionVectorSelector vectorSelector;
  private final SingleIndexedInt currentRow = new SingleIndexedInt();

  private int[] rowVector;
  private int currentId = ReadableVectorInspector.NULL_ID;

  public ShimSingleValueDimensionSelector(
      final ShimCursor cursor,
      final SingleValueDimensionVectorSelector vectorSelector
  )
  {
    this.cursor = cursor;
    this.vectorInspector = cursor.vectorColumnSelectorFactory.getReadableVectorInspector();
    this.vectorSelector = vectorSelector;
  }

  @Override
  public IndexedInts getRow()
  {
    populateRowVector();
    currentRow.setValue(rowVector[cursor.currentIndexInVector]);
    return currentRow;
  }

  @Nullable
  @Override
  public Object getObject()
  {
    populateRowVector();
    return lookupName(rowVector[cursor.currentIndexInVector]);
  }

  @Override
  public ValueMatcher makeValueMatcher(@Nullable String value)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
  }

  @Override
  public ValueMatcher makeValueMatcher(DruidPredicateFactory predicateFactory)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicateFactory);
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // Don't bother.
  }

  @Override
  public Class<?> classOfObject()
  {
    return String.class;
  }

  @Override
  public int getValueCardinality()
  {
    return vectorSelector.getValueCardinality();
  }

  @Override
  public boolean supportsLookupNameUtf8()
  {
    return vectorSelector.supportsLookupNameUtf8();
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    return vectorSelector.lookupName(id);
  }

  @Nullable
  @Override
  public ByteBuffer lookupNameUtf8(int id)
  {
    return vectorSelector.lookupNameUtf8(id);
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return vectorSelector.nameLookupPossibleInAdvance();
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return vectorSelector.idLookup();
  }

  private void populateRowVector()
  {
    final int id = vectorInspector.getId();
    if (id != currentId) {
      rowVector = vectorSelector.getRowVector();
      currentId = id;
    }
  }
}
