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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.BaseColumn;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ComplexColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Describes the basic shape for any 'nested data' ({@link StructuredData}) {@link ComplexColumn} implementation along
 * with basic facilities for caching any columns created and methods for retrieving selectors for nested literal field
 * columns.
 */
public abstract class NestedDataComplexColumn implements ComplexColumn
{
  @Nullable
  public static NestedDataComplexColumn fromColumnSelector(
      ColumnSelector columnSelector,
      String columnName
  )
  {
    ColumnHolder holder = columnSelector.getColumnHolder(columnName);
    if (holder == null) {
      return null;
    }
    BaseColumn theColumn = holder.getColumn();
    if (theColumn instanceof CompressedNestedDataComplexColumn) {
      return (CompressedNestedDataComplexColumn) theColumn;
    }
    throw new IAE(
        "Column [%s] is invalid type, found [%s] instead of [%s]",
        columnName,
        theColumn.getClass(),
        NestedDataComplexColumn.class.getSimpleName()
    );
  }


  /**
   * Make a {@link DimensionSelector} for a nested literal field column associated with this nested
   * complex column specified by a sequence of {@link NestedPathPart}.
   */
  public abstract DimensionSelector makeDimensionSelector(
      List<NestedPathPart> path,
      ReadableOffset readableOffset,
      ExtractionFn fn
  );

  /**
   * Make a {@link ColumnValueSelector} for a nested literal field column associated with this nested
   * complex column specified by a sequence of {@link NestedPathPart}.
   */
  public abstract ColumnValueSelector<?> makeColumnValueSelector(
      List<NestedPathPart> path,
      ReadableOffset readableOffset
  );

  /**
   * Make a {@link SingleValueDimensionVectorSelector} for a nested literal field column associated with this nested
   * complex column specified by a sequence of {@link NestedPathPart}.
   */
  public abstract SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
      List<NestedPathPart> path,
      ReadableVectorOffset readableOffset
  );

  /**
   * Make a {@link VectorObjectSelector} for a nested literal field column associated with this nested
   * complex column located at the 'path' represented as a sequence of {@link NestedPathPart}.
   */
  public abstract VectorObjectSelector makeVectorObjectSelector(
      List<NestedPathPart> path,
      ReadableVectorOffset readableOffset
  );

  /**
   * Make a {@link VectorValueSelector} for a nested literal field column associated with this nested
   * complex column located at the 'path' represented as a sequence of {@link NestedPathPart}.
   */
  public abstract VectorValueSelector makeVectorValueSelector(
      List<NestedPathPart> path,
      ReadableVectorOffset readableOffset
  );

  /**
   * Make a {@link ColumnIndexSupplier} for a nested literal field column associated with this nested
   * complex column located at the 'path' represented as a sequence of {@link NestedPathPart}.
   */
  @Nullable
  public abstract ColumnIndexSupplier getColumnIndexSupplier(List<NestedPathPart> path);

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
