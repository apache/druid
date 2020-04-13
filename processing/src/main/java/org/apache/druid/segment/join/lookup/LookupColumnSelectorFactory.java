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

package org.apache.druid.segment.join.lookup;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseSingleValueDimensionSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnCapabilitiesImpl;
import org.apache.druid.segment.column.ValueType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.function.Supplier;

public class LookupColumnSelectorFactory implements ColumnSelectorFactory
{
  public static final String KEY_COLUMN = "k";
  public static final String VALUE_COLUMN = "v";

  private final Supplier<Pair<String, String>> currentEntry;

  LookupColumnSelectorFactory(
      final Supplier<Pair<String, String>> currentEntry
  )
  {
    this.currentEntry = currentEntry;
  }

  @Nonnull
  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    final Supplier<String> supplierToUse;

    if (KEY_COLUMN.equals(dimensionSpec.getDimension())) {
      supplierToUse = () -> {
        final Pair<String, String> entry = currentEntry.get();
        return entry != null ? entry.lhs : null;
      };
    } else if (VALUE_COLUMN.equals(dimensionSpec.getDimension())) {
      supplierToUse = () -> {
        final Pair<String, String> entry = currentEntry.get();
        return entry != null ? entry.rhs : null;
      };
    } else {
      return DimensionSelector.constant(null, dimensionSpec.getExtractionFn());
    }

    return dimensionSpec.decorate(
        new BaseSingleValueDimensionSelector()
        {
          @Nullable
          @Override
          protected String getValue()
          {
            if (dimensionSpec.getExtractionFn() != null) {
              return dimensionSpec.getExtractionFn().apply(supplierToUse.get());
            } else {
              return supplierToUse.get();
            }
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
            inspector.visit("dimensionSpec", dimensionSpec);
            inspector.visit("supplier", supplierToUse);
          }
        }
    );
  }

  @Nonnull
  @Override
  public ColumnValueSelector makeColumnValueSelector(String columnName)
  {
    return makeDimensionSelector(DefaultDimensionSpec.of(columnName));
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    if (LookupJoinable.ALL_COLUMNS.contains(column)) {
      return new ColumnCapabilitiesImpl().setType(ValueType.STRING);
    } else {
      return null;
    }
  }
}
