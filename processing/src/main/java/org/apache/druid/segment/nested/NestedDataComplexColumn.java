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


package org.apache.druid.segment.nested;

import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Set;

/**
 * Describes the basic shape for any 'nested data' ({@link StructuredData}) {@link ComplexColumn} implementation along
 * with basic facilities for caching any columns created and methods for retrieving selectors for nested field columns.
 * <p>
 * {@link org.apache.druid.segment.virtual.NestedFieldVirtualColumn} allows query time use of the nested fields.
 */
public abstract class NestedDataComplexColumn implements ComplexColumn
{
  /**
   * Make a {@link DimensionSelector} for a nested field column
   */
  public abstract DimensionSelector makeDimensionSelector(
      List<NestedPathPart> path,
      ReadableOffset readableOffset,
      ExtractionFn fn
  );

  /**
   * Make a {@link ColumnValueSelector} for a nested field column
   */
  public abstract ColumnValueSelector<?> makeColumnValueSelector(
      List<NestedPathPart> path,
      ReadableOffset readableOffset
  );

  /**
   * Make a {@link SingleValueDimensionVectorSelector} for a nested field column
   */
  public abstract SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
      List<NestedPathPart> path,
      ReadableVectorOffset readableOffset
  );

  /**
   * Make a {@link VectorObjectSelector} for a nested field column
   */
  public abstract VectorObjectSelector makeVectorObjectSelector(
      List<NestedPathPart> path,
      ReadableVectorOffset readableOffset
  );

  /**
   * Make a {@link VectorValueSelector} for a nested field column
   */
  public abstract VectorValueSelector makeVectorValueSelector(
      List<NestedPathPart> path,
      ReadableVectorOffset readableOffset
  );

  /**
   * Get list of fields represented as a sequence of {@link NestedPathPart}
   */
  public abstract List<List<NestedPathPart>> getNestedFields();

  /**
   * Get all {@link ColumnType} for the nested field column
   */
  @Nullable
  public abstract Set<ColumnType> getColumnTypes(List<NestedPathPart> path);

  /**
   * Get a {@link ColumnHolder} for a nested field column to retrieve metadata, the column itself, or indexes.
   */
  @Nullable
  public abstract ColumnHolder getColumnHolder(List<NestedPathPart> path);

  /**
   * Make a {@link ColumnIndexSupplier} for a nested field column
   */
  @Nullable
  public abstract ColumnIndexSupplier getColumnIndexSupplier(List<NestedPathPart> path);

  /**
   * Shortcut to check if a nested field column is {@link ColumnType#isNumeric()}, useful when broadly choosing the
   * type of vector selector to be used when dealing with the path
   */
  public abstract boolean isNumeric(List<NestedPathPart> path);

  @Override
  public Class<?> getClazz()
  {
    return StructuredData.class;
  }

  @Override
  public String getTypeName()
  {
    return NestedDataComplexTypeSerde.TYPE_NAME;
  }
}
