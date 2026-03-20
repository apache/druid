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

package org.apache.druid.segment.virtual;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.expression.NestedDataExpressions;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.ColumnSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnIndexSupplier;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.SelectableColumn;
import org.apache.druid.segment.data.ReadableOffset;
import org.apache.druid.segment.nested.NestedColumnIndexSupplier;
import org.apache.druid.segment.nested.NestedColumnSelectorFactory;
import org.apache.druid.segment.nested.NestedColumnTypeInspector;
import org.apache.druid.segment.nested.NestedPathField;
import org.apache.druid.segment.nested.NestedPathPart;
import org.apache.druid.segment.nested.NestedVectorColumnSelectorFactory;
import org.apache.druid.segment.serde.NoIndexesColumnIndexSupplier;
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Specialized virtual column for {@link NestedDataExpressions.JsonMergeExprMacro}.
 */
@JsonTypeName("nested-merge")
public class NestedMergeVirtualColumn extends SpecializedExpressionVirtualColumn
{
  private final List<String> columns;

  @JsonCreator
  public NestedMergeVirtualColumn(
      @JsonProperty("name") String name,
      @JsonProperty("columns") List<String> columns,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    super(
        new ExpressionVirtualColumn(
            name,
            StringUtils.format(
                "%s(%s)",
                NestedDataExpressions.JsonMergeExprMacro.NAME,
                columns.stream().map(column -> Parser.identifier(column).stringify()).collect(Collectors.joining(","))
            ),
            ColumnType.NESTED_DATA,
            macroTable
        )
    );

    this.columns = columns;
  }

  @JsonProperty
  public List<String> getColumns()
  {
    return columns;
  }

  @Override
  public SelectableColumn toSelectableColumn(final ColumnIndexSelector columnSelector)
  {
    return new SelectableColumn()
    {
      @Override
      public <T> T as(Class<T> clazz)
      {
        if (NestedColumnTypeInspector.class.equals(clazz)
            || NestedColumnSelectorFactory.class.equals(clazz)
            || NestedVectorColumnSelectorFactory.class.equals(clazz)
            || NestedColumnIndexSupplier.class.equals(clazz)) {
          return (T) createNestedBundle(columnSelector);
        }

        return null;
      }
    };
  }

  @Override
  public String toString()
  {
    return "NestedMergeVirtualColumn{" +
           "name='" + getOutputName() + '\'' +
           ", columns=" + columns +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    NestedMergeVirtualColumn that = (NestedMergeVirtualColumn) o;
    return Objects.equals(columns, that.columns);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), columns);
  }

  /**
   * Creates a nested column view of this virtual column, if all underlying columns are themselves nested columns.
   * Otherwise, returns null so overall evaluation falls back to expression evaluation.
   */
  @Nullable
  AsNestedColumn createNestedBundle(final ColumnSelector columnSelector)
  {
    final Map<String, SelectableColumn> keyToColumnMap = new HashMap<>();
    final List<SelectableColumn> selectableColumns = new ArrayList<>();
    for (String column : columns) {
      final ColumnHolder columnHolder = columnSelector.getColumnHolder(column);
      final SelectableColumn selectableColumn = columnHolder.getColumn();
      final NestedColumnTypeInspector typeInspector = selectableColumn.as(NestedColumnTypeInspector.class);
      if (typeInspector == null) {
        // Not a nested column; return null so we fall back to expression evaluation.
        return null;
      }

      final List<List<NestedPathPart>> fields = typeInspector.getNestedFields();
      for (List<NestedPathPart> field : fields) {
        if (!field.isEmpty()) {
          if (field.get(0) instanceof NestedPathField) {
            keyToColumnMap.put(field.get(0).getPartIdentifier(), selectableColumn);
          } else {
            // Root level is not an object; return null so we fall back to expression evaluation.
            return null;
          }
        }
      }

      selectableColumns.add(selectableColumn);
    }
    return new AsNestedColumn(columnSelector, selectableColumns, keyToColumnMap);
  }

  /**
   * Nested-column view of this virtual column.
   */
  class AsNestedColumn implements
      NestedColumnTypeInspector,
      NestedVectorColumnSelectorFactory,
      NestedColumnSelectorFactory,
      NestedColumnIndexSupplier
  {
    private final ColumnSelector columnSelector;
    private final List<SelectableColumn> selectableColumns;
    private final Map<String, SelectableColumn> keyToColumnMap;

    public AsNestedColumn(
        final ColumnSelector columnSelector,
        final List<SelectableColumn> selectableColumns,
        final Map<String, SelectableColumn> keyToColumnMap
    )
    {
      this.columnSelector = columnSelector;
      this.selectableColumns = selectableColumns;
      this.keyToColumnMap = keyToColumnMap;
    }

    @Override
    public List<List<NestedPathPart>> getNestedFields()
    {
      final List<List<NestedPathPart>> retVal = new ArrayList<>();
      for (final SelectableColumn column : selectableColumns) {
        final NestedColumnTypeInspector typeInspector = column.as(NestedColumnTypeInspector.class);
        if (typeInspector != null) {
          final List<List<NestedPathPart>> fields = typeInspector.getNestedFields();
          for (List<NestedPathPart> field : fields) {
            if (!field.isEmpty()
                && (field.get(0) instanceof NestedPathField)
                && keyToColumnMap.containsKey(field.get(0).getPartIdentifier())) {
              retVal.add(field);
            }
          }
        }
      }
      return retVal;
    }

    @Override
    @Nullable
    public Set<ColumnType> getFieldTypes(List<NestedPathPart> path)
    {
      if (path.isEmpty()) {
        return Set.of(ColumnType.NESTED_DATA);
      }

      final SelectableColumn column = getColumnForPath(path);
      if (column != null) {
        final NestedColumnTypeInspector typeInspector = column.as(NestedColumnTypeInspector.class);
        if (typeInspector != null) {
          return typeInspector.getFieldTypes(path);
        }
      }

      return Set.of();
    }

    @Override
    @Nullable
    public ColumnType getFieldLogicalType(List<NestedPathPart> path)
    {
      if (path.isEmpty()) {
        return ColumnType.NESTED_DATA;
      }

      final SelectableColumn column = getColumnForPath(path);
      if (column != null) {
        final NestedColumnTypeInspector typeInspector = column.as(NestedColumnTypeInspector.class);
        if (typeInspector != null) {
          return typeInspector.getFieldLogicalType(path);
        }
      }

      return null;
    }

    @Override
    public boolean isNumeric(List<NestedPathPart> path)
    {
      if (path.isEmpty()) {
        return false;
      }

      final SelectableColumn column = getColumnForPath(path);
      if (column != null) {
        final NestedColumnTypeInspector typeInspector = column.as(NestedColumnTypeInspector.class);
        if (typeInspector != null) {
          return typeInspector.isNumeric(path);
        }
      }

      return false;
    }

    @Override
    public DimensionSelector makeDimensionSelector(
        List<NestedPathPart> path,
        @Nullable ExtractionFn extractionFn,
        ColumnSelectorFactory columnSelectorFactory,
        @Nullable ReadableOffset readableOffset
    )
    {
      if (path.isEmpty()) {
        // Fall back to expression evaluation for the root path.
        return NestedMergeVirtualColumn.this.makeDimensionSelector(
            DefaultDimensionSpec.of(NestedMergeVirtualColumn.this.getOutputName()),
            columnSelectorFactory,
            columnSelector,
            readableOffset
        );
      }

      final SelectableColumn column = getColumnForPath(path);
      if (column != null) {
        final NestedColumnSelectorFactory factory = column.as(NestedColumnSelectorFactory.class);
        if (factory != null) {
          return factory.makeDimensionSelector(path, extractionFn, columnSelectorFactory, readableOffset);
        }
      }

      return DimensionSelector.constant(null, extractionFn);
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(
        List<NestedPathPart> path,
        ColumnSelectorFactory columnSelectorFactory,
        @Nullable ReadableOffset readableOffset
    )
    {
      if (path.isEmpty()) {
        // Fall back to expression evaluation for the root path.
        return NestedMergeVirtualColumn.this.makeColumnValueSelector(
            NestedMergeVirtualColumn.this.getOutputName(),
            columnSelectorFactory,
            columnSelector,
            readableOffset
        );
      }

      final SelectableColumn column = getColumnForPath(path);
      if (column != null) {
        final NestedColumnSelectorFactory factory = column.as(NestedColumnSelectorFactory.class);
        if (factory != null) {
          return factory.makeColumnValueSelector(path, columnSelectorFactory, readableOffset);
        }
      }

      return NilColumnValueSelector.instance();
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
        List<NestedPathPart> path,
        VectorColumnSelectorFactory columnSelectorFactory,
        ReadableVectorOffset readableOffset
    )
    {
      if (path.isEmpty()) {
        // Fall back to expression evaluation for the root path.
        return NestedMergeVirtualColumn.this.makeSingleValueVectorDimensionSelector(
            DefaultDimensionSpec.of(NestedMergeVirtualColumn.this.getOutputName()),
            columnSelectorFactory,
            columnSelector,
            readableOffset
        );
      }

      final SelectableColumn column = getColumnForPath(path);
      if (column != null) {
        final NestedVectorColumnSelectorFactory factory = column.as(NestedVectorColumnSelectorFactory.class);
        if (factory != null) {
          return factory.makeSingleValueDimensionVectorSelector(path, columnSelectorFactory, readableOffset);
        }
      }

      return NilVectorSelector.create(readableOffset);
    }

    @Override
    public VectorObjectSelector makeVectorObjectSelector(
        List<NestedPathPart> path,
        VectorColumnSelectorFactory columnSelectorFactory,
        ReadableVectorOffset readableOffset
    )
    {
      if (path.isEmpty()) {
        // Fall back to expression evaluation for the root path.
        return NestedMergeVirtualColumn.this.makeVectorObjectSelector(
            NestedMergeVirtualColumn.this.getOutputName(),
            columnSelectorFactory,
            columnSelector,
            readableOffset
        );
      }

      final SelectableColumn column = getColumnForPath(path);
      if (column != null) {
        final NestedVectorColumnSelectorFactory factory = column.as(NestedVectorColumnSelectorFactory.class);
        if (factory != null) {
          return factory.makeVectorObjectSelector(path, columnSelectorFactory, readableOffset);
        }
      }

      return NilVectorSelector.create(readableOffset);
    }

    @Override
    public VectorValueSelector makeVectorValueSelector(
        List<NestedPathPart> path,
        VectorColumnSelectorFactory columnSelectorFactory,
        ReadableVectorOffset readableOffset
    )
    {
      if (path.isEmpty()) {
        // Fall back to expression evaluation for the root path.
        return NestedMergeVirtualColumn.this.makeVectorValueSelector(
            NestedMergeVirtualColumn.this.getOutputName(),
            columnSelectorFactory,
            columnSelector,
            readableOffset
        );
      }

      final SelectableColumn column = getColumnForPath(path);
      if (column != null) {
        final NestedVectorColumnSelectorFactory factory = column.as(NestedVectorColumnSelectorFactory.class);
        if (factory != null) {
          return factory.makeVectorValueSelector(path, columnSelectorFactory, readableOffset);
        }
      }

      return NilVectorSelector.create(readableOffset);
    }

    @Override
    @Nullable
    public ColumnIndexSupplier getColumnIndexSupplier(List<NestedPathPart> path)
    {
      if (path.isEmpty()) {
        // No indexes for the root path.
        return NoIndexesColumnIndexSupplier.getInstance();
      }

      final SelectableColumn column = getColumnForPath(path);
      if (column != null) {
        final NestedColumnIndexSupplier supplier = column.as(NestedColumnIndexSupplier.class);
        if (supplier != null) {
          return supplier.getColumnIndexSupplier(path);
        } else {
          return NoIndexesColumnIndexSupplier.getInstance();
        }
      } else {
        return null;
      }
    }

    @Nullable
    private SelectableColumn getColumnForPath(List<NestedPathPart> path)
    {
      if (path.isEmpty()) {
        throw DruidException.defensive("Empty path should be handled by the caller, not here.");
      } else if (path.get(0) instanceof NestedPathField) {
        return keyToColumnMap.get(path.get(0).getPartIdentifier());
      } else {
        return null;
      }
    }
  }
}
