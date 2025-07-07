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

import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * Implementation of {@link ColumnSelectorFactory} for {@link ShimCursor}.
 */
public class ShimColumnSelectorFactory implements ColumnSelectorFactory
{
  private final ShimCursor cursor;
  private final Map<DimensionSpec, DimensionSelector> dimensionSelectors = new HashMap<>();
  private final Map<String, ColumnValueSelector<Object>> columnValueSelectors = new HashMap<>();

  public ShimColumnSelectorFactory(ShimCursor cursor)
  {
    this.cursor = cursor;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    return dimensionSelectors.computeIfAbsent(
        dimensionSpec,
        spec -> {
          // The following call can fail if makeDimensionSelector is called on a non-dictionary-encoded string column.
          // To support this case, we'd need to provide a bridge from cursor.vectorColumnSelectorFactory.makeObjectSelector.
          final SingleValueDimensionVectorSelector vectorSelector =
              cursor.vectorColumnSelectorFactory.makeSingleValueDimensionSelector(spec);
          return new ShimSingleValueDimensionSelector(cursor, vectorSelector);
        }
    );
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(String columnName)
  {
    return columnValueSelectors.computeIfAbsent(
        columnName,
        column -> {
          final ColumnCapabilities capabilities = cursor.vectorColumnSelectorFactory.getColumnCapabilities(column);
          if (capabilities == null) {
            //noinspection unchecked
            return NilColumnValueSelector.instance();
          } else if (capabilities.is(ValueType.STRING)) {
            return makeDimensionSelector(DefaultDimensionSpec.of(columnName));
          } else if (capabilities.isNumeric()) {
            return new ShimNumericColumnValueSelector(
                cursor,
                cursor.vectorColumnSelectorFactory.makeValueSelector(columnName),
                capabilities.getType()
            );
          } else {
            return new ShimObjectColumnValueSelector(
                cursor,
                cursor.vectorColumnSelectorFactory.makeObjectSelector(columnName)
            );
          }
        }
    );
  }

  @Override
  @Nullable
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return cursor.vectorColumnSelectorFactory.getColumnCapabilities(column);
  }
}
