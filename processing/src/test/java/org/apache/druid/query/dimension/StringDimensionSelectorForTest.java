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

package org.apache.druid.query.dimension;

import com.google.common.base.Predicate;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.objects.Object2IntMap;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.AbstractDimensionSelector;
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.ArrayBasedIndexedInts;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Supplier;

public class StringDimensionSelectorForTest extends AbstractDimensionSelector
{
  private final Supplier<List<String>> rowSupplier;
  private final boolean unknownCardinality;
  private final boolean validIdLookup;
  private final boolean nameLookupPossibleInAdvance;
  private final Object2IntMap<String> dictionary;
  private final Int2ObjectMap<String> reverseDictionary;

  private final ArrayBasedIndexedInts currentRow = new ArrayBasedIndexedInts();

  public StringDimensionSelectorForTest(
      Supplier<List<String>> rowSupplier,
      Object2IntMap<String> dictionary,
      Int2ObjectMap<String> reverseDictionary,
      boolean unknownCardinality,
      boolean validIdLookup,
      boolean nameLookupPossibleInAdvance
  )
  {
    this.rowSupplier = rowSupplier;
    this.unknownCardinality = unknownCardinality;
    this.validIdLookup = validIdLookup;
    this.nameLookupPossibleInAdvance = nameLookupPossibleInAdvance;
    this.dictionary = dictionary;
    this.reverseDictionary = reverseDictionary;
  }

  @Override
  public int getValueCardinality()
  {
    return unknownCardinality ? DimensionDictionarySelector.CARDINALITY_UNKNOWN : dictionary.size();
  }

  @Nullable
  @Override
  public String lookupName(int id)
  {
    return reverseDictionary.get(id);
  }

  @Override
  public boolean nameLookupPossibleInAdvance()
  {
    return nameLookupPossibleInAdvance;
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return validIdLookup ? dictionary::getInt : null;
  }

  @Override
  public IndexedInts getRow()
  {
    List<String> multiValues = rowSupplier.get();
    currentRow.ensureSize(multiValues.size());
    for (int i = 0; i < multiValues.size(); i++) {
      currentRow.setValue(i, dictionary.getInt(multiValues.get(i)));
    }
    currentRow.setSize(multiValues.size());
    return currentRow;
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
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
  }

  @Override
  public Class<?> classOfObject()
  {
    return Object.class;
  }
}
