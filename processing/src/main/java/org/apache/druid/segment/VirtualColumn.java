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
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.FallbackVirtualColumn;
import org.apache.druid.segment.virtual.ListFilteredVirtualColumn;

import javax.annotation.Nullable;
import java.util.List;

/**
 * Virtual columns are "views" created over a {@link ColumnSelectorFactory} or {@link ColumnSelector}. They can
 * potentially draw from multiple underlying columns, although they always present themselves as if they were a single
 * column.
 *
 * A virtual column object will be shared amongst threads and must be thread safe. The selectors returned
 * from the various makeXXXSelector methods need not be thread safe.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "expression", value = ExpressionVirtualColumn.class),
    @JsonSubTypes.Type(name = "fallback", value = FallbackVirtualColumn.class),
    @JsonSubTypes.Type(name = "mv-filtered", value = ListFilteredVirtualColumn.class)
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
   */
  DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory);

  /**
   * Returns similar {@link DimensionSelector} object as returned by
   * {@link #makeDimensionSelector(DimensionSpec, ColumnSelectorFactory)} except this method has full access to the
   * underlying column and can potentially provide a more efficient implementation.
   *
   * Users of this interface must ensure to first call this method whenever possible. Typically this can not be called
   * in query paths on top of IncrementalIndex which doesn't have columns as in persisted segments.
   */
  @SuppressWarnings("unused")
  @Nullable
  default DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    return null;
  }

  /**
   * Build a {@link ColumnValueSelector} corresponding to this virtual column. Also provides the name that the
   * virtual column was referenced with, which is useful if this column uses dot notation.
   */
  ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelectorFactory factory);

  /**
   * Returns similar {@link ColumnValueSelector} object as returned by
   * {@link #makeColumnValueSelector(String, ColumnSelectorFactory)} except this method has full access to the
   * underlying column and can potentially provide a more efficient implementation.
   *
   * Users of this interface must ensure to first call this method whenever possible. Typically this can not be called
   * in query paths on top of IncrementalIndex which doesn't have columns as in persisted segments.
   */
  @SuppressWarnings("unused")
  @Nullable
  default ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableOffset offset
  )
  {
    return null;
  }

  default boolean canVectorize(ColumnInspector inspector)
  {
    return false;
  }

  /**
   * Build a {@link SingleValueDimensionVectorSelector} corresponding to this virtual column. Also provides the name
   * that the virtual column was referenced with (through {@link DimensionSpec#getDimension()}, which is useful if this
   * column uses dot notation. The virtual column is expected to apply any necessary decoration from the
   * {@link DimensionSpec}.
   */
  default SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory
  )
  {
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * Returns similar {@link SingleValueDimensionVectorSelector} object as returned by
   * {@link #makeSingleValueVectorDimensionSelector(DimensionSpec, ColumnSelector, ReadableVectorOffset)} except this
   * method has full access to the underlying column and can potentially provide a more efficient implementation.
   *
   * Users of this interface must ensure to first call this method whenever possible.
   */
  @SuppressWarnings("unused")
  @Nullable
  default SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    return null;
  }

  /**
   * Build a {@link MultiValueDimensionVectorSelector} corresponding to this virtual column. Also provides
   * the name that the virtual column was referenced with (through {@link DimensionSpec#getDimension()}, which is useful
   * if this column uses dot notation. The virtual column is expected to apply any necessary decoration from the
   * {@link DimensionSpec}.
   */
  default MultiValueDimensionVectorSelector makeMultiValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory
  )
  {
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * Returns similar {@link SingleValueDimensionVectorSelector} object as returned by
   * {@link #makeSingleValueVectorDimensionSelector(DimensionSpec, ColumnSelector, ReadableVectorOffset)} except this
   * method has full access to the underlying column and can potentially provide a more efficient implementation.
   *
   * Users of this interface must ensure to first call this method whenever possible.
   */
  @SuppressWarnings("unused")
  @Nullable
  default MultiValueDimensionVectorSelector makeMultiValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    return null;
  }


  /**
   * Build a {@link VectorValueSelector} corresponding to this virtual column. Also provides the name that the
   * virtual column was referenced with, which is useful if this column uses dot notation.
   */
  default VectorValueSelector makeVectorValueSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * Returns similar {@link VectorValueSelector} object as returned by
   * {@link #makeVectorValueSelector(String, VectorColumnSelectorFactory)} except this method has full access to the
   * underlying column and can potentially provide a more efficient implementation.
   *
   * Users of this interface must ensure to first call this method whenever possible.
   */
  @SuppressWarnings("unused")
  @Nullable
  default VectorValueSelector makeVectorValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    return null;
  }

  /**
   * Build a {@link VectorObjectSelector} corresponding to this virtual column. Also provides the name that the
   * virtual column was referenced with, which is useful if this column uses dot notation.
   */
  default VectorObjectSelector makeVectorObjectSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * Returns similar {@link VectorObjectSelector} object as returned by
   * {@link #makeVectorObjectSelector(String, VectorColumnSelectorFactory)} except this method has full access to the
   * underlying column and can potentially provide a more efficient implementation.
   *
   * Users of this interface must ensure to first call this method whenever possible.
   */
  @SuppressWarnings("unused")
  @Nullable
  default VectorObjectSelector makeVectorObjectSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    return null;
  }

  /**
   * This method is deprecated in favor of {@link #capabilities(ColumnInspector, String)}, which should be used whenever
   * possible and can support virtual column implementations that need to inspect other columns as inputs.
   *
   * This is a fallback implementation to return the capabilities of this virtual column, which includes a type that
   * corresponds to the best performing base selector supertype (e. g. {@link BaseLongColumnValueSelector}) of the
   * object, returned from {@link #makeColumnValueSelector(String, ColumnSelectorFactory)}. May vary based on columnName
   * if this column uses dot notation.
   *
   * @param columnName the name this virtual column was referenced with
   *
   * @return capabilities, must not be null
   */
  @Deprecated
  ColumnCapabilities capabilities(String columnName);

  /**
   * Return the {@link ColumnCapabilities} which best describe the optimal selector to read from this virtual column.
   * <p>
   * The {@link ColumnInspector} (most likely corresponding to an underlying {@link ColumnSelectorFactory} of a query)
   * allows the virtual column to consider this information if necessary to compute its output type details.
   * <p>
   * Examples of this include the {@link ExpressionVirtualColumn}, which takes input from other columns and uses the
   * {@link ColumnInspector} to infer the output type of expressions based on the types of the inputs.
   *
   * @param inspector column inspector to provide additional information of other available columns
   * @param columnName the name this virtual column was referenced with
   * @return capabilities, must not be null
   */
  @Nullable
  default ColumnCapabilities capabilities(ColumnInspector inspector, String columnName)
  {
    return capabilities(columnName);
  }

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
   * Get the {@link ColumnIndexSupplier} for the specified virtual column, with the assistance of a
   * {@link ColumnIndexSelector} to allow reading things from segments. If the virtual column has no indexes, this
   * method will return null, or may also return a non-null supplier whose methods may return null values - having a
   * supplier is no guarantee that the column has indexes.
   */
  @SuppressWarnings("unused")
  @Nullable
  default ColumnIndexSupplier getIndexSupplier(
      String columnName,
      ColumnIndexSelector columnIndexSelector
  )
  {
    return NoIndexesColumnIndexSupplier.getInstance();
  }
}
