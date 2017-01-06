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

package io.druid.query.groupby;

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.Row;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.extraction.ExtractionFn;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.column.Column;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;
import io.druid.segment.data.RangeIndexedInts;
import io.druid.segment.data.ZeroIndexedInts;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public class RowBasedColumnSelectorFactory implements ColumnSelectorFactory
{
  private final Supplier<? extends Row> row;
  private final Map<String, ValueType> rowSignature;

  private RowBasedColumnSelectorFactory(
      final Supplier<? extends Row> row,
      @Nullable final Map<String, ValueType> rowSignature
  )
  {
    this.row = row;
    this.rowSignature = rowSignature != null ? rowSignature : ImmutableMap.<String, ValueType>of();
  }

  public static RowBasedColumnSelectorFactory create(
      final Supplier<? extends Row> row,
      @Nullable final Map<String, ValueType> rowSignature
  )
  {
    return new RowBasedColumnSelectorFactory(row, rowSignature);
  }

  public static RowBasedColumnSelectorFactory create(
      final ThreadLocal<? extends Row> row,
      @Nullable final Map<String, ValueType> rowSignature
  )
  {
    return new RowBasedColumnSelectorFactory(
        new Supplier<Row>()
        {
          @Override
          public Row get()
          {
            return row.get();
          }
        },
        rowSignature
    );
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    // This dimension selector does not have an associated lookup dictionary, which means lookup can only be done
    // on the same row. Hence it returns CARDINALITY_UNKNOWN from getValueCardinality.
    return dimensionSpec.decorate(makeDimensionSelectorUndecorated(dimensionSpec));
  }

  private DimensionSelector makeDimensionSelectorUndecorated(DimensionSpec dimensionSpec)
  {
    final String dimension = dimensionSpec.getDimension();
    final ExtractionFn extractionFn = dimensionSpec.getExtractionFn();

    if (Column.TIME_COLUMN_NAME.equals(dimensionSpec.getDimension())) {
      if (extractionFn == null) {
        throw new UnsupportedOperationException("time dimension must provide an extraction function");
      }

      return new DimensionSelector()
      {
        @Override
        public IndexedInts getRow()
        {
          return ZeroIndexedInts.instance();
        }

        @Override
        public int getValueCardinality()
        {
          return DimensionSelector.CARDINALITY_UNKNOWN;
        }

        @Override
        public String lookupName(int id)
        {
          return extractionFn.apply(row.get().getTimestampFromEpoch());
        }

        @Override
        public int lookupId(String name)
        {
          throw new UnsupportedOperationException("lookupId");
        }
      };
    } else {
      return new DimensionSelector()
      {
        @Override
        public IndexedInts getRow()
        {
          final List<String> dimensionValues = row.get().getDimension(dimension);
          return RangeIndexedInts.create(dimensionValues != null ? dimensionValues.size() : 0);
        }

        @Override
        public int getValueCardinality()
        {
          return DimensionSelector.CARDINALITY_UNKNOWN;
        }

        @Override
        public String lookupName(int id)
        {
          final String value = Strings.emptyToNull(row.get().getDimension(dimension).get(id));
          return extractionFn == null ? value : extractionFn.apply(value);
        }

        @Override
        public int lookupId(String name)
        {
          throw new UnsupportedOperationException("lookupId");
        }
      };
    }
  }

  @Override
  public FloatColumnSelector makeFloatColumnSelector(final String columnName)
  {
    if (columnName.equals(Column.TIME_COLUMN_NAME)) {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          return (float) row.get().getTimestampFromEpoch();
        }
      };
    } else {
      return new FloatColumnSelector()
      {
        @Override
        public float get()
        {
          return row.get().getFloatMetric(columnName);
        }
      };
    }
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(final String columnName)
  {
    if (columnName.equals(Column.TIME_COLUMN_NAME)) {
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          return row.get().getTimestampFromEpoch();
        }
      };
    } else {
      return new LongColumnSelector()
      {
        @Override
        public long get()
        {
          return row.get().getLongMetric(columnName);
        }
      };
    }
  }

  @Override
  public ObjectColumnSelector makeObjectColumnSelector(final String columnName)
  {
    if (columnName.equals(Column.TIME_COLUMN_NAME)) {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return Long.class;
        }

        @Override
        public Object get()
        {
          return row.get().getTimestampFromEpoch();
        }
      };
    } else {
      return new ObjectColumnSelector()
      {
        @Override
        public Class classOfObject()
        {
          return Object.class;
        }

        @Override
        public Object get()
        {
          return row.get().getRaw(columnName);
        }
      };
    }
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String columnName)
  {
    if (Column.TIME_COLUMN_NAME.equals(columnName)) {
      // TIME_COLUMN_NAME is handled specially; override the provided rowSignature.
      return new ColumnCapabilitiesImpl().setType(ValueType.LONG);
    } else {
      final ValueType valueType = rowSignature.get(columnName);

      // Do _not_ set isDictionaryEncoded or hasBitmapIndexes, because Row-based columns do not have those things.
      return valueType != null ? new ColumnCapabilitiesImpl().setType(valueType) : null;
    }
  }
}
