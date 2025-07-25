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
import org.apache.druid.segment.DimensionDictionarySelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;
import org.apache.druid.segment.data.ZeroIndexedInts;
import org.apache.druid.segment.vector.VectorObjectSelector;

import javax.annotation.Nullable;

/**
 * {@link DimensionSelector} that internally uses a {@link VectorObjectSelector}. Does not support any dictionary
 * operations.
 */
public class ShimVectorObjectDimSelector extends ShimObjectColumnValueSelector implements DimensionSelector
{
  public ShimVectorObjectDimSelector(ShimCursor shimCursor, VectorObjectSelector vectorObjectSelector)
  {
    super(shimCursor, vectorObjectSelector);
  }

  @Override
  public IndexedInts getRow()
  {
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
    throw new UnsupportedOperationException("ShimVectorObjectDimSelector does not support lookupName");
  }

  @Nullable
  @Override
  public IdLookup idLookup()
  {
    return null;
  }
}
