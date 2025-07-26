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
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.RangeIndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;
import org.apache.druid.segment.vector.ReadableVectorInspector;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;
import java.util.ArrayList;

/**
 * {@link DimensionSelector} that internally uses a {@link VectorObjectSelector}. Does not support any dictionary
 * operations.
 */
public class ShimVectorObjectDimSelector implements DimensionSelector
{
  private final ShimCursor cursor;
  private final ReadableVectorInspector vectorInspector;
  private final VectorObjectSelector vectorSelector;
  private final boolean hasMultipleValues;

  private Object[] objectVector;
  private int objectId = ReadableVectorInspector.NULL_ID;

  public ShimVectorObjectDimSelector(
      final ShimCursor cursor,
      final VectorObjectSelector vectorSelector,
      boolean hasMultipleValues
  )
  {
    this.cursor = cursor;
    this.vectorInspector = cursor.vectorColumnSelectorFactory.getReadableVectorInspector();
    this.vectorSelector = vectorSelector;
    this.hasMultipleValues = hasMultipleValues;
  }

  @Nullable
  @Override
  public Object getObject()
  {
    populateObjectVector();
    return objectVector[cursor.currentIndexInVector];
  }

  @Override
  public Class<?> classOfObject()
  {
    return Object.class;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // Don't bother.
  }

  private void populateObjectVector()
  {
    final int id = vectorInspector.getId();
    if (id != objectId) {
      objectVector = vectorSelector.getObjectVector();
      objectId = id;
    }
  }

  @Override
  public IndexedInts getRow()
  {
    if (hasMultipleValues) {
      Object object = getObject();
      ArrayList arrayList = (ArrayList) object;
      RangeIndexedInts rangeIndexedInts = new RangeIndexedInts();
      rangeIndexedInts.setSize(arrayList.size());
      return rangeIndexedInts;
    }
    return ZeroIndexedInts.instance();
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
  public boolean nameLookupPossibleInAdvance()
  {
    return false;
  }

  @Override
  public int getValueCardinality()
  {
    return DimensionDictionarySelector.CARDINALITY_UNKNOWN;
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    Object object = getObject();
    if (hasMultipleValues) {
      ArrayList arrayList = (ArrayList) object;
      object = arrayList.get(id);
    }
    return object == null ? null : object.toString();
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return null;
  }
}
