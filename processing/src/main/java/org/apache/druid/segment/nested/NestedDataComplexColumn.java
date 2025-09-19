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

import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ComplexColumn;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Describes the basic shape for any 'nested data' ({@link StructuredData}) {@link ComplexColumn} implementation along
 * with basic facilities for caching any columns created and methods for retrieving selectors for nested field columns.
 * <p>
 * {@link org.apache.druid.segment.virtual.NestedFieldVirtualColumn} allows query time use of the nested fields.
 */
public abstract class NestedDataComplexColumn implements
    ComplexColumn,
    NestedColumnTypeInspector,
    NestedColumnIndexSupplier,
    NestedColumnSelectorFactory,
    NestedVectorColumnSelectorFactory
{
  /**
   * Get a {@link ColumnHolder} for a nested field column to retrieve metadata, the column itself, or indexes.
   */
  @Nullable
  public abstract ColumnHolder getColumnHolder(List<NestedPathPart> path);

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

  @Override
  @Nullable
  public <T> T as(Class<T> clazz)
  {
    if (NestedColumnTypeInspector.class.equals(clazz)
        || NestedColumnIndexSupplier.class.equals(clazz)
        || NestedColumnSelectorFactory.class.equals(clazz)
        || NestedVectorColumnSelectorFactory.class.equals(clazz)) {
      return (T) this;
    } else {
      return null;
    }
  }
}
