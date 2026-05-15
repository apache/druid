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

package org.apache.druid.frame.processor;

import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.DruidPredicateFactory;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.DimensionSelectorUtils;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

/**
 * {@link DimensionSelector} that delegates to a selector obtained from a {@link ColumnSelectorFactory} supplier.
 * The delegate is refreshed when the supplier returns a different factory (by identity). This allows the selector
 * to remain valid across changes to the underlying data source, such as frame changes during merging or combining.
 */
public class TrackingDimensionSelector implements DimensionSelector
{
  private final DimensionSpec dimensionSpec;
  private final Supplier<ColumnSelectorFactory> factorySupplier;
  private ColumnSelectorFactory delegateFactory;
  private DimensionSelector delegate;

  public TrackingDimensionSelector(final DimensionSpec dimensionSpec, final Supplier<ColumnSelectorFactory> factorySupplier)
  {
    this.dimensionSpec = dimensionSpec;
    this.factorySupplier = factorySupplier;
  }

  private DimensionSelector delegate()
  {
    final ColumnSelectorFactory currentFactory = factorySupplier.get();
    //noinspection ObjectEquality
    if (currentFactory != delegateFactory) {
      delegateFactory = currentFactory;
      delegate = currentFactory.makeDimensionSelector(dimensionSpec);
    }
    return delegate;
  }

  @Override
  public IndexedInts getRow()
  {
    return delegate().getRow();
  }

  @Override
  public ValueMatcher makeValueMatcher(@Nullable final String value)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, value);
  }

  @Override
  public ValueMatcher makeValueMatcher(final DruidPredicateFactory predicateFactory)
  {
    return DimensionSelectorUtils.makeValueMatcherGeneric(this, predicateFactory);
  }

  @Nullable
  @Override
  public Object getObject()
  {
    return delegate().getObject();
  }

  @Override
  public Class<?> classOfObject()
  {
    return delegate().classOfObject();
  }

  @Override
  public int getValueCardinality()
  {
    return CARDINALITY_UNKNOWN;
  }

  @Nullable
  @Override
  public String lookupName(final int id)
  {
    return delegate().lookupName(id);
  }

  @Nullable
  @Override
  public ByteBuffer lookupNameUtf8(final int id)
  {
    return delegate().lookupNameUtf8(id);
  }

  @Override
  public boolean supportsLookupNameUtf8()
  {
    return delegate().supportsLookupNameUtf8();
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
  public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
  {
    // Do nothing.
  }
}
