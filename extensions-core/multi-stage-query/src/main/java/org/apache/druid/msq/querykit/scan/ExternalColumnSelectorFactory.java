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

package org.apache.druid.msq.querykit.scan;

import com.google.common.base.Predicate;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ValueMatcher;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.IdLookup;
import org.apache.druid.segment.RowIdSupplier;
import org.apache.druid.segment.SimpleSettableOffset;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.IndexedInts;

import javax.annotation.Nullable;

/**
 * A column selector factory wrapper which wraps the underlying factory's errors into a {@link ParseException}.
 * This is used when reading from external data, since failure to read the data is usually an issue with the external
 * input
 */
public class ExternalColumnSelectorFactory implements ColumnSelectorFactory
{
  private static final String ERROR_MESSAGE_FORMAT_STRING =
      "Error while trying to read the external data source at inputSource [%s], rowNumber [%d], columnName [%s]";

  private final ColumnSelectorFactory delegate;
  private final InputSource inputSource;
  private final SimpleSettableOffset offset;

  public ExternalColumnSelectorFactory(
      final ColumnSelectorFactory delgate,
      final InputSource inputSource,
      final SimpleSettableOffset offset
  )
  {
    this.delegate = delgate;
    this.inputSource = inputSource;
    this.offset = offset;
  }

  @Override
  public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
  {
    return new DimensionSelector()
    {
      final DimensionSelector delegateDimensionSelector = delegate.makeDimensionSelector(dimensionSpec);

      @Override
      public IndexedInts getRow()
      {
        return delegateDimensionSelector.getRow();
      }

      @Override
      public ValueMatcher makeValueMatcher(@Nullable String value)
      {
        return delegateDimensionSelector.makeValueMatcher(value);
      }

      @Override
      public ValueMatcher makeValueMatcher(Predicate<String> predicate)
      {
        return delegateDimensionSelector.makeValueMatcher(predicate);
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        delegateDimensionSelector.inspectRuntimeShape(inspector);
      }

      @Nullable
      @Override
      public Object getObject()
      {
        try {
          return delegateDimensionSelector.getObject();
        }
        catch (Exception e) {
          throw createException(e, dimensionSpec.getDimension(), inputSource, offset);
        }
      }

      @Override
      public Class<?> classOfObject()
      {
        return delegateDimensionSelector.classOfObject();
      }

      @Override
      public int getValueCardinality()
      {
        return delegateDimensionSelector.getValueCardinality();
      }

      @Nullable
      @Override
      public String lookupName(int id)
      {
        return delegateDimensionSelector.lookupName(id);
      }

      @Override
      public boolean nameLookupPossibleInAdvance()
      {
        return delegateDimensionSelector.nameLookupPossibleInAdvance();
      }

      @Nullable
      @Override
      public IdLookup idLookup()
      {
        return delegateDimensionSelector.idLookup();
      }
    };
  }

  @Override
  public ColumnValueSelector makeColumnValueSelector(String columnName)
  {
    return new ColumnValueSelector()
    {
      final ColumnValueSelector delegateColumnValueSelector = delegate.makeColumnValueSelector(columnName);

      @Override
      public double getDouble()
      {
        try {
          return delegateColumnValueSelector.getDouble();
        }
        catch (Exception e) {
          throw createException(e, columnName, inputSource, offset);
        }
      }

      @Override
      public float getFloat()
      {
        try {
          return delegateColumnValueSelector.getFloat();
        }
        catch (Exception e) {
          throw createException(e, columnName, inputSource, offset);
        }
      }

      @Override
      public long getLong()
      {
        try {
          return delegateColumnValueSelector.getLong();
        }
        catch (Exception e) {
          throw createException(e, columnName, inputSource, offset);
        }
      }

      @Override
      public void inspectRuntimeShape(RuntimeShapeInspector inspector)
      {
        delegateColumnValueSelector.inspectRuntimeShape(inspector);
      }

      @Override
      public boolean isNull()
      {
        return delegateColumnValueSelector.isNull();
      }

      @Nullable
      @Override
      public Object getObject()
      {
        try {
          return delegateColumnValueSelector.getObject();
        }
        catch (Exception e) {
          throw createException(e, columnName, inputSource, offset);
        }
      }

      @Override
      public Class classOfObject()
      {
        return delegateColumnValueSelector.classOfObject();
      }
    };
  }

  @Nullable
  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return delegate.getColumnCapabilities(column);
  }

  @Nullable
  @Override
  public RowIdSupplier getRowIdSupplier()
  {
    return delegate.getRowIdSupplier();
  }

  public static ParseException createException(
      Exception cause,
      String columnName,
      InputSource inputSource,
      SimpleSettableOffset offset
  )
  {
    return new ParseException(
        null,
        cause,
        ERROR_MESSAGE_FORMAT_STRING,
        inputSource.toString(),
        (long) (offset.getOffset()) + 1,
        columnName
    );
  }
}
