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
import org.apache.druid.annotations.SubclassesMustOverrideEqualsAndHashCode;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.query.groupby.DeferExpressionDimensions;
import org.apache.druid.query.groupby.epinephelinae.vector.GroupByVectorColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.SelectableColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.nested.NestedColumnSelectorFactory;
import org.apache.druid.segment.nested.NestedColumnTypeInspector;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;
import org.apache.druid.segment.vector.MultiValueDimensionVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.apache.druid.segment.virtual.FallbackVirtualColumn;
import org.apache.druid.segment.virtual.FeaturelessSelectableColumn;
import org.apache.druid.segment.virtual.ListFilteredVirtualColumn;
import org.apache.druid.segment.virtual.PrefixFilteredVirtualColumn;
import org.apache.druid.segment.virtual.RegexFilteredVirtualColumn;

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
    @JsonSubTypes.Type(name = "mv-filtered", value = ListFilteredVirtualColumn.class),
    @JsonSubTypes.Type(name = "mv-regex-filtered", value = RegexFilteredVirtualColumn.class),
    @JsonSubTypes.Type(name = "mv-prefix-filtered", value = PrefixFilteredVirtualColumn.class)
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
   * Build a selector corresponding to this virtual column.
   *
   * The virtual column is expected to apply any necessary {@link DimensionSpec#decorate(DimensionSelector)} or
   * {@link DimensionSpec#getExtractionFn()} from the dimensionSpec.
   *
   * @param dimensionSpec         spec the column was referenced with. Also provides the name that the
   *                              virtual column was referenced with, which is useful if this column uses dot notation.
   * @param columnSelectorFactory object for fetching underlying selectors.
   * @param columnSelector        object for fetching underlying columns, if available. Generally only available for
   *                              regular segments.
   * @param offset                offset to use with underlying columns. Available only if columnSelector is available.
   */
  default DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelectorFactory columnSelectorFactory,
      @Nullable ColumnSelector columnSelector,
      @Nullable ReadableOffset offset
  )
  {
    // Implementation for backwards compatibility with existing extensions.
    if (columnSelector != null) {
      final DimensionSelector selector = makeDimensionSelector(dimensionSpec, columnSelector, offset);
      if (selector != null) {
        return selector;
      }
    }
    return makeDimensionSelector(dimensionSpec, columnSelectorFactory);
  }

  /**
   * @deprecated use {@link #makeDimensionSelector(DimensionSpec, ColumnSelectorFactory, ColumnSelector, ReadableOffset)}
   */
  @Deprecated
  default DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec, ColumnSelectorFactory factory)
  {
    throw DruidException
        .forPersona(DruidException.Persona.DEVELOPER)
        .ofCategory(DruidException.Category.UNSUPPORTED)
        .build("Implement makeDimensionSelector(DimensionSpec, ColumnSelector, ColumnSelectorFactory, ReadableOffset) "
               + "for[%s]", getClass());
  }

  /**
   * @deprecated use {@link #makeDimensionSelector(DimensionSpec, ColumnSelectorFactory, ColumnSelector, ReadableOffset)}
   */
  @Deprecated
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
   * Builds a selector corresponding to this virtual column.
   *
   * @param columnName            name the column was referenced with, which is useful if this column uses dot notation.
   * @param columnSelectorFactory object for fetching underlying selectors.
   * @param columnSelector        object for fetching underlying columns, if available. Generally only available for
   *                              regular segments.
   * @param offset                offset to use with underlying columns. Available only if columnSelector is available.
   */
  default ColumnValueSelector<?> makeColumnValueSelector(
      String columnName,
      ColumnSelectorFactory columnSelectorFactory,
      @Nullable ColumnSelector columnSelector,
      @Nullable ReadableOffset offset
  )
  {
    // Implementation for backwards compatibility with existing extensions.
    if (columnSelector != null && offset != null) {
      final ColumnValueSelector<?> selector = makeColumnValueSelector(columnName, columnSelector, offset);
      if (selector != null) {
        return selector;
      }
    }
    return makeColumnValueSelector(columnName, columnSelectorFactory);
  }

  /**
   * @deprecated use {@link #makeColumnValueSelector(String, ColumnSelectorFactory, ColumnSelector, ReadableOffset)}
   */
  @Deprecated
  default ColumnValueSelector<?> makeColumnValueSelector(String columnName, ColumnSelectorFactory factory)
  {
    throw DruidException
        .forPersona(DruidException.Persona.DEVELOPER)
        .ofCategory(DruidException.Category.UNSUPPORTED)
        .build("Implement makeColumnValueSelector(String, ColumnSelectorFactory, ColumnSelector, ReadableOffset) "
               + "for[%s]", getClass());
  }

  /**
   * @deprecated use {@link #makeColumnValueSelector(String, ColumnSelectorFactory, ColumnSelector, ReadableOffset)}
   */
  @Deprecated
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
   * Build a selector corresponding to this virtual column.
   *
   * The virtual column is expected to apply any necessary {@link DimensionSpec#decorate(DimensionSelector)} or
   * {@link DimensionSpec#getExtractionFn()} from the dimensionSpec.
   *
   * @param dimensionSpec  spec the column was referenced with. Also provides the name that the
   *                       virtual column was referenced with, which is useful if this column uses dot notation.
   * @param factory        object for fetching underlying selectors.
   * @param columnSelector object for fetching underlying columns.
   * @param offset         offset to use with underlying columns.
   */
  default SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    // Implementation for backwards compatibility with existing extensions.
    final SingleValueDimensionVectorSelector selector =
        makeSingleValueVectorDimensionSelector(dimensionSpec, columnSelector, offset);
    if (selector != null) {
      return selector;
    } else {
      return makeSingleValueVectorDimensionSelector(dimensionSpec, factory);
    }
  }

  /**
   * @deprecated use {@link #makeSingleValueVectorDimensionSelector(DimensionSpec, ColumnSelector, ReadableVectorOffset)}
   */
  @Deprecated
  default SingleValueDimensionVectorSelector makeSingleValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory
  )
  {
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * @deprecated use {@link #makeSingleValueVectorDimensionSelector(DimensionSpec, ColumnSelector, ReadableVectorOffset)}
   */
  @Deprecated
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
   * Build a selector corresponding to this virtual column.
   *
   * The virtual column is expected to apply any necessary {@link DimensionSpec#decorate(DimensionSelector)} or
   * {@link DimensionSpec#getExtractionFn()} from the dimensionSpec.
   *
   * @param dimensionSpec  spec the column was referenced with. Also provides the name that the
   *                       virtual column was referenced with, which is useful if this column uses dot notation.
   * @param factory        object for fetching underlying selectors.
   * @param columnSelector object for fetching underlying columns, if available. Generally only available for
   *                       regular segments.
   * @param offset         offset to use with underlying columns. Available only if columnSelector is available.
   */
  default MultiValueDimensionVectorSelector makeMultiValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    // Implementation for backwards compatibility with existing extensions.
    final MultiValueDimensionVectorSelector selector =
        makeMultiValueVectorDimensionSelector(dimensionSpec, columnSelector, offset);
    if (selector != null) {
      return selector;
    } else {
      return makeMultiValueVectorDimensionSelector(dimensionSpec, factory);
    }
  }

  /**
   * @deprecated use {@link #makeMultiValueVectorDimensionSelector(DimensionSpec, ColumnSelector, ReadableVectorOffset)}
   */
  @Deprecated
  default MultiValueDimensionVectorSelector makeMultiValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      VectorColumnSelectorFactory factory
  )
  {
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * @deprecated use {@link #makeMultiValueVectorDimensionSelector(DimensionSpec, ColumnSelector, ReadableVectorOffset)}
   */
  @Nullable
  @Deprecated
  default MultiValueDimensionVectorSelector makeMultiValueVectorDimensionSelector(
      DimensionSpec dimensionSpec,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    return null;
  }

  /**
   * Build a selector corresponding to this virtual column.
   *
   * The virtual column is expected to apply any necessary {@link DimensionSpec#decorate(DimensionSelector)} or
   * {@link DimensionSpec#getExtractionFn()} from the dimensionSpec.
   *
   * @param columnName     name the column was referenced with, which is useful if this column uses dot notation.
   * @param factory        object for fetching underlying selectors.
   * @param columnSelector object for fetching underlying columns.
   * @param offset         offset to use with underlying columns.
   */
  default VectorValueSelector makeVectorValueSelector(
      String columnName,
      VectorColumnSelectorFactory factory,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    // Implementation for backwards compatibility with existing extensions.
    final VectorValueSelector selector = makeVectorValueSelector(columnName, columnSelector, offset);
    if (selector != null) {
      return selector;
    } else {
      return makeVectorValueSelector(columnName, factory);
    }
  }

  /**
   * @deprecated use {@link #makeVectorValueSelector(String, ColumnSelector, ReadableVectorOffset)}
   */
  @Deprecated
  default VectorValueSelector makeVectorValueSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * @deprecated use {@link #makeVectorValueSelector(String, ColumnSelector, ReadableVectorOffset)}
   */
  @Nullable
  @Deprecated
  default VectorValueSelector makeVectorValueSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    return null;
  }

  /**
   * Build a selector corresponding to this virtual column.
   *
   * The virtual column is expected to apply any necessary {@link DimensionSpec#decorate(DimensionSelector)} or
   * {@link DimensionSpec#getExtractionFn()} from the dimensionSpec.
   *
   * @param columnName     name the column was referenced with, which is useful if this column uses dot notation.
   * @param factory        object for fetching underlying selectors.
   * @param columnSelector object for fetching underlying columns, if available. Generally only available for
   *                       regular segments.
   * @param offset         offset to use with underlying columns. Available only if columnSelector is available.
   */
  default VectorObjectSelector makeVectorObjectSelector(
      String columnName,
      VectorColumnSelectorFactory factory,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    // Implementation for backwards compatibility with existing extensions.
    final VectorObjectSelector selector = makeVectorObjectSelector(columnName, columnSelector, offset);
    if (selector != null) {
      return selector;
    } else {
      return makeVectorObjectSelector(columnName, factory);
    }
  }

  /**
   * @deprecated use {@link #makeVectorObjectSelector(String, ColumnSelector, ReadableVectorOffset)}
   */
  @Deprecated
  default VectorObjectSelector makeVectorObjectSelector(String columnName, VectorColumnSelectorFactory factory)
  {
    throw new UnsupportedOperationException("not supported");
  }

  /**
   * @deprecated use {@link #makeVectorObjectSelector(String, ColumnSelector, ReadableVectorOffset)}
   */
  @Nullable
  @Deprecated
  default VectorObjectSelector makeVectorObjectSelector(
      String columnName,
      ColumnSelector columnSelector,
      ReadableVectorOffset offset
  )
  {
    return null;
  }

  /**
   * Returns a group-by selector. Allows virtual columns to control their own grouping behavior.
   *
   * @param columnName                column name
   * @param factory                   column selector factory
   * @param deferExpressionDimensions active value of {@link org.apache.druid.query.groupby.GroupByQueryConfig#CTX_KEY_DEFER_EXPRESSION_DIMENSIONS}
   *
   * @return selector, or null if this virtual column does not have a specialized one
   */
  @SuppressWarnings("unused")
  @Nullable
  default GroupByVectorColumnSelector makeGroupByVectorColumnSelector(
      String columnName,
      VectorColumnSelectorFactory factory,
      DeferExpressionDimensions deferExpressionDimensions
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
   * @param inspector  column inspector to provide additional information of other available columns
   * @param columnName the name this virtual column was referenced with
   *
   * @return capabilities, or null if the column should be treated as if it doesn't exist.
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
  default boolean usesDotNotation()
  {
    return false;
  }

  /**
   * Produces a {@link SelectableColumn} from this virtual column. This allows the virtual column to offer specialized
   * interfaces, such as {@link NestedColumnTypeInspector} or {@link NestedColumnSelectorFactory}. These interfaces
   * may be used by query objects, including other virtual columns, to perform queries in a more efficient manner.
   *
   * @param columnSelector selector for underlying columns and indexes. Cannot be used during execution of this method,
   *                       but can be stored for later usage.
   */
  default SelectableColumn toSelectableColumn(ColumnIndexSelector columnSelector)
  {
    return FeaturelessSelectableColumn.INSTANCE;
  }

  /**
   * Get the {@link ColumnIndexSupplier} of the specified virtual column, with the assistance of a
   * {@link ColumnSelector} to allow reading things from segments. Returns null if the virtual column wants to
   * act like a missing column. Returns {@link NoIndexesColumnIndexSupplier#getInstance()} if the virtual
   * column does not support indexes and wants cursor-based filtering.
   */
  @Nullable
  default ColumnIndexSupplier getIndexSupplier(
      String columnName,
      ColumnIndexSelector columnIndexSelector
  )
  {
    return NoIndexesColumnIndexSupplier.getInstance();
  }

  /**
   * Returns a key used for "equivalence" comparisons, for checking if some virtual column is equivalent to some other
   * virtual column, regardless of the output name. If this method returns null, it does not participate in equivalence
   * comparisons.
   *
   * @see VirtualColumns#findEquivalent(VirtualColumn)
   */
  @Nullable
  default EquivalenceKey getEquivalanceKey()
  {
    return null;
  }

  @SubclassesMustOverrideEqualsAndHashCode
  interface EquivalenceKey
  {
  }
}
