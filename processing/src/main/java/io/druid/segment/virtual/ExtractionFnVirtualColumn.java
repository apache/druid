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

package io.druid.segment.virtual;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Floats;
import io.druid.common.guava.GuavaUtils;
import io.druid.query.cache.CacheKeyBuilder;
import io.druid.query.dimension.DefaultDimensionSpec;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.CascadeExtractionFn;
import io.druid.query.extraction.ExtractionFn;
import io.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import io.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Objects;

/**
 * A VirtualColumn that applies an extractionFn to another column. If the other column is multi-value, this
 * column will be multi-value as well.
 */
public class ExtractionFnVirtualColumn implements VirtualColumn
{
  private static final ColumnCapabilities CAPABILITIES = new ColumnCapabilitiesImpl().setType(ValueType.STRING);

  private final String name;
  private final String fieldName;
  private final ExtractionFn extractionFn;

  @JsonCreator
  public ExtractionFnVirtualColumn(
      @JsonProperty("name") final String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("extractionFn") final ExtractionFn extractionFn
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.fieldName = Preconditions.checkNotNull(fieldName, "fieldName");
    this.extractionFn = Preconditions.checkNotNull(extractionFn, "extractionFn");
  }

  @Override
  @JsonProperty("name")
  public String getOutputName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public ExtractionFn getExtractionFn()
  {
    return extractionFn;
  }

  @Override
  public ObjectColumnSelector makeObjectColumnSelector(final String columnName, final ColumnSelectorFactory factory)
  {
    // Make a dimension selector and then transform the returned values to objects.
    final DimensionSelector dimensionSelector = makeDimensionSelector(
        new DefaultDimensionSpec(columnName, columnName),
        factory
    );

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
        // Return either String or String[], like QueryableIndexStorageAdapter would.
        final IndexedInts row = dimensionSelector.getRow();
        if (row.size() == 0) {
          return null;
        } else if (row.size() == 1) {
          return dimensionSelector.lookupName(row.get(0));
        } else {
          final String[] strings = new String[row.size()];
          for (int i = 0; i < row.size(); i++) {
            strings[i] = dimensionSelector.lookupName(row.get(i));
          }
          return strings;
        }
      }
    };
  }

  @NotNull
  @Override
  public DimensionSelector makeDimensionSelector(final DimensionSpec dimensionSpec, final ColumnSelectorFactory factory)
  {
    // Combine our extractionFn + dimensionSpec's extractionFn (if any)
    final ExtractionFn combinedExtractionFn;
    if (dimensionSpec.getExtractionFn() == null) {
      combinedExtractionFn = extractionFn;
    } else {
      combinedExtractionFn = new CascadeExtractionFn(new ExtractionFn[]{extractionFn, dimensionSpec.getExtractionFn()});
    }

    return dimensionSpec.decorate(
        factory.makeDimensionSelector(
            new ExtractionDimensionSpec(fieldName, fieldName, ValueType.STRING, combinedExtractionFn)
        )
    );
  }

  @Nullable
  @Override
  public FloatColumnSelector makeFloatColumnSelector(final String columnName, final ColumnSelectorFactory factory)
  {
    final FloatColumnSelector baseSelector = factory.makeFloatColumnSelector(fieldName);
    return new FloatColumnSelector()
    {
      @Override
      public float get()
      {
        final Float theFloat = Floats.tryParse(extractionFn.apply(baseSelector.get()));
        return theFloat != null ? theFloat : 0f;
      }

      @Override
      public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
      {
        inspector.visit("baseSelector", baseSelector);
        inspector.visit("extractionFn", extractionFn);
      }
    };
  }

  @Nullable
  @Override
  public LongColumnSelector makeLongColumnSelector(final String columnName, final ColumnSelectorFactory factory)
  {
    final LongColumnSelector baseSelector = factory.makeLongColumnSelector(fieldName);
    return new LongColumnSelector()
    {
      @Override
      public long get()
      {
        final Long theLong = GuavaUtils.tryParseLong(extractionFn.apply(baseSelector.get()));
        return theLong != null ? theLong : 0L;
      }

      @Override
      public void inspectRuntimeShape(final RuntimeShapeInspector inspector)
      {
        inspector.visit("baseSelector", baseSelector);
        inspector.visit("extractionFn", extractionFn);
      }
    };
  }

  @Override
  public ColumnCapabilities capabilities(final String columnName)
  {
    return CAPABILITIES;
  }

  @Override
  public List<String> requiredColumns()
  {
    return ImmutableList.of(fieldName);
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(VirtualColumnCacheHelper.CACHE_TYPE_ID_EXTRACTION_FN)
        .appendString(name)
        .appendString(fieldName)
        .appendCacheable(extractionFn)
        .build();
  }

  @Override
  public boolean equals(final Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    final ExtractionFnVirtualColumn that = (ExtractionFnVirtualColumn) o;
    return Objects.equals(name, that.name) &&
           Objects.equals(fieldName, that.fieldName) &&
           Objects.equals(extractionFn, that.extractionFn);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(name, fieldName, extractionFn);
  }

  @Override
  public String toString()
  {
    return "ExtractionFnVirtualColumn{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", extractionFn=" + extractionFn +
           '}';
  }
}
