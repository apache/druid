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

package io.druid.segment;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.java.util.common.Cacheable;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.virtual.ExpressionVirtualColumn;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Virtual columns are "views" created over a ColumnSelectorFactory. They can potentially draw from multiple
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
   * virtual column was referenced with, which is useful if this column uses dot notation.
   *
   * @param columnName the name this virtual column was referenced with
   * @param factory    column selector factory
   *
   * @return the selector, must not be null
   */
  ObjectColumnSelector makeObjectColumnSelector(String columnName, ColumnSelectorFactory factory);

  /**
   * Build a selector corresponding to this virtual column. Also provides the name that the
   * virtual column was referenced with (through {@link DimensionSpec#getDimension()}, which
   * is useful if this column uses dot notation. The virtual column is expected to apply any
   * necessary decoration from the dimensionSpec.
   *
   * @param dimensionSpec the dimensionSpec this column was referenced with
   * @param factory       column selector factory
   *
   * @return the selector, or null if we can't make a selector
   */
  @Nullable
  DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory);

  /**
   * Build a selector corresponding to this virtual column. Also provides the name that the
   * virtual column was referenced with, which is useful if this column uses dot notation.
   *
   * @param columnName the name this virtual column was referenced with
   * @param factory    column selector factory
   *
   * @return the selector, or null if we can't make a selector
   */
  @Nullable
  FloatColumnSelector makeFloatColumnSelector(String columnName, ColumnSelectorFactory factory);

  /**
   * Build a selector corresponding to this virtual column. Also provides the name that the
   * virtual column was referenced with, which is useful if this column uses dot notation.
   *
   * @param columnName the name this virtual column was referenced with
   * @param factory    column selector factory
   *
   * @return the selector, or null if we can't make a selector
   */
  @Nullable
  LongColumnSelector makeLongColumnSelector(String columnName, ColumnSelectorFactory factory);

  /**
   * Build a selector corresponding to this virtual column. Also provides the name that the
   * virtual column was referenced with, which is useful if this column uses dot notation.
   *
   * @param columnName the name this virtual column was referenced with
   * @param factory    column selector factory
   *
   * @return the selector, or null if we can't make a selector
   */
  @Nullable
  DoubleColumnSelector makeDoubleColumnSelector(String columnName, ColumnSelectorFactory factory);

  /**
   * Returns the capabilities of this virtual column, which includes a type that should match
   * the type returned by "makeObjectColumnSelector" and should correspond to the best
   * performing selector. May vary based on columnName if this column uses dot notation.
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
}
