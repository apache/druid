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

package org.apache.druid.segment;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.column.BitmapIndex;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Virtual columns are "views" created over a ColumnSelectorFactory or ColumnSelector. They can potentially draw from multiple
 * underlying columns, although they always present themselves as if they were a single column.
 *
 * A virtual column object will be shared amongst threads and must be thread safe. The selectors returned
 * from the various makeXXXSelector methods need not be thread safe.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "expression", value = ExpressionVirtualColumn.class)
})
public interface VirtualColumn extends Cacheable
{
  /**
   * Output name of this column.
   *
   * @return name
   */
  String getOutputName();

  /**
   * Build a selector corresponding to this virtual column. Also provides the name that the
   * virtual column was referenced with (through {@link DimensionSpec#getDimension()}, which
   * is useful if this column uses dot notation. The virtual column is expected to apply any
   * necessary decoration from the dimensionSpec.
   *
   * @param dimensionSpec the dimensionSpec this column was referenced with
   * @param factory       column selector factory
   *
   * @return the selector, must not be null
   */
  DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory);

  /**
   * Returns similar DimensionSelector object as returned by {@link #makeDimensionSelector(DimensionSpec, ColumnSelectorFactory)}
   * except this method has full access to underlying column and can potentially provide a more efficient implementation.
   *
   * Users of this interface must ensure to first call this method whenever possible. Typically this can not be called in
   * query paths on top of IncrementalIndex which doesn't have columns as in persisted segments.
   *
   * @param dimensionSpec
   * @param columnSelector
   * @param offset
   * @return the selector
   */
  @SuppressWarnings("unused")
  @Nullable
  default DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelector columnSelector, ReadableOffset offset)
  {
    return null;
  }

  /**
   * Build a selector corresponding to this virtual column. Also provides the name that the
   * virtual column was referenced with, which is useful if this column uses dot notation.
   *
   * @param columnName the name this virtual column was referenced with
   * @param factory    column selector factory
   *
   * @return the selector, must not be null
   */
  ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelectorFactory factory);

  /**
   * Returns similar ColumnValueSelector object as returned by {@link #makeColumnValueSelector(String, ColumnSelectorFactory)}
   * except this method has full access to underlying column and can potentially provide a more efficient implementation.
   *
   * Users of this interface must ensure to first call this method whenever possible. Typically this can not be called in
   * query paths on top of IncrementalIndex which doesn't have columns as in persisted segments.
   *
   * @param columnName
   * @param columnSelector
   * @param offset
   * @return the selector
   */
  @SuppressWarnings("unused")
  @Nullable
  default ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelector columnSelector, ReadableOffset offset)
  {
    return null;
  }

  /**
   * Returns the capabilities of this virtual column, which includes a type that corresponds to the best
   * performing base selector supertype (e. g. {@link BaseLongColumnValueSelector}) of the object, returned from
   * {@link #makeColumnValueSelector(String, ColumnSelectorFactory)}. May vary based on columnName if this column uses
   * dot notation.
   *
   * @param columnName the name this virtual column was referenced with
   *
   * @return capabilities, must not be null
   */
  ColumnCapabilities capabilities(String columnName);

  /**
   * Returns a list of columns that this virtual column will access. This may include the
   * names of other virtual columns. May be empty if a virtual column doesn't access any
   * underlying columns.
   *
   * Does not pass columnName because there is an assumption that the list of columns
   * needed by a dot-notation supporting virtual column will not vary based on the
   * columnName.
   *
   * @return column names
   */
  List<String> requiredColumns();

  /**
   * Indicates that this virtual column can be referenced with dot notation. For example,
   * a virtual column named "foo" could be referred to as "foo.bar" with the Cursor it is
   * registered with. In that case, init will be called with columnName "foo.bar" rather
   * than "foo".
   *
   * @return whether to use dot notation
   */
  boolean usesDotNotation();

  /**
   * Returns the BitmapIndex for efficient filtering on columns that support it. This method is only used if
   * {@link ColumnCapabilities} returned from {@link #capabilities(String)} has flag for BitmapIndex support.
   * @param columnName
   * @param selector
   * @return BitmapIndex
   */
  @SuppressWarnings("unused")
  default BitmapIndex getBitmapIndex(String columnName, ColumnSelector selector)
  {
    throw new UnsupportedOperationException("not supported");
  }
}
