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
import com.google.common.base.Strings;
import io.druid.query.filter.ValueMatcher;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.ZeroIndexedInts;
import io.druid.segment.filter.BooleanValueMatcher;
import io.druid.segment.historical.SingleValueHistoricalDimensionSelector;

import javax.annotation.Nullable;

public class NullDimensionSelector implements SingleValueHistoricalDimensionSelector, IdLookup
{
  private static final NullDimensionSelector INSTANCE = new NullDimensionSelector();

  private NullDimensionSelector()
  {
    // Singleton.
  }

  public static NullDimensionSelector instance()
  {
    return INSTANCE;
  }

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
  public int getRowValue(int offset)
  {
    return 0;
  }

  @Override
  public IndexedInts getRow(int offset)
  {
    return getRow();
  }

  @Override
  public ValueMatcher makeValueMatcher(String value)
  {
    return BooleanValueMatcher.of(value == null);
  }

  @Override
  public ValueMatcher makeValueMatcher(Predicate<String> predicate)
  {
    return BooleanValueMatcher.of(predicate.apply(null));
  }

  @Override
  public int getValueCardinality()
  {
    return 1;
  }

  @Override
  public String lookupName(int id)
  {
    assert id == 0 : "id = " + id;
    return null;
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
    return this;
  }

  @Override
  public int lookupId(String name)
  {
    return Strings.isNullOrEmpty(name) ? 0 : -1;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {
    // nothing to inspect
  }
}
