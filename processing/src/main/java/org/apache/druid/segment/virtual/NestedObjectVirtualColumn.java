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
import com.google.common.base.Preconditions;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.math.expr.Parser;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.expression.NestedDataExpressions;
import org.apache.druid.query.extraction.ExtractionFn;
import org.apache.druid.query.filter.ColumnIndexSelector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.NilColumnValueSelector;
import org.apache.druid.segment.VirtualColumn;
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
import org.apache.druid.segment.vector.NilVectorSelector;
import org.apache.druid.segment.vector.ReadableVectorOffset;
import org.apache.druid.segment.vector.SingleValueDimensionVectorSelector;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorObjectSelector;
import org.apache.druid.segment.vector.VectorValueSelector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Specialized virtual column for {@link NestedDataExpressions.JsonObjectExprMacro}.
 */
@JsonTypeName("nested-object")
public class NestedObjectVirtualColumn extends SpecializedExpressionVirtualColumn
{
  private final Map<String, TypedExpression> keyExprMap;
  private final ExprMacroTable macroTable;

  @JsonCreator
  public NestedObjectVirtualColumn(
      @JsonProperty("name") String name,
      @JsonProperty("object") Map<String, TypedExpression> keyExprMap,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    super(
        new ExpressionVirtualColumn(
            name,
            StringUtils.format(
                "%s(%s)",
                NestedDataExpressions.JsonObjectExprMacro.NAME,
                Preconditions.checkNotNull(keyExprMap, "object")
                             .entrySet().stream().map(entry -> {
                               final String key = entry.getKey();
                               final TypedExpression valueExpr = entry.getValue();
                               return Parser.constant(key).stringify() + ',' + valueExpr.expression;
                             }).collect(Collectors.joining(","))
            ),
            ColumnType.NESTED_DATA,
            macroTable
        )
    );

    this.keyExprMap = keyExprMap;
    this.macroTable = macroTable;
  }

  @JsonProperty("object")
  public Map<String, TypedExpression> getKeyExprMap()
  {
    return keyExprMap;
  }

  @Override
  public SelectableColumn toSelectableColumn(final ColumnIndexSelector columnSelector)
  {
    if (keyExprMap.values().stream().allMatch(te -> te.type.isPrimitive())) {
      return new SelectableColumn()
      {
        @Override
        public <T> T as(Class<T> clazz)
        {
          if (NestedColumnTypeInspector.class.equals(clazz)
              || NestedColumnSelectorFactory.class.equals(clazz)
              || NestedVectorColumnSelectorFactory.class.equals(clazz)
              || NestedColumnIndexSupplier.class.equals(clazz)) {
            return (T) new AsNestedColumn(columnSelector);
          }

          return null;
        }
      };
    } else {
      return super.toSelectableColumn(columnSelector);
    }
  }

  @Override
  public String toString()
  {
    return "NestedObjectVirtualColumn{" +
           "name='" + getOutputName() + '\'' +
           ", object=" + keyExprMap +
           '}';
  }

  /**
   * Nested column view of the json_object function. Only usable for single-level objects, where all
   * values are primitives.
   */
  class AsNestedColumn implements
      NestedColumnTypeInspector,
      NestedVectorColumnSelectorFactory,
      NestedColumnSelectorFactory,
      NestedColumnIndexSupplier
  {
    private final ColumnIndexSelector columnSelector;

    /**
     * Map of field name to result of {@link #getVirtualColumnForExpression(List, TypedExpression)}.
     */
    private final Map<String, VirtualColumn> keyColumnMap = new HashMap<>();

    public AsNestedColumn(final ColumnIndexSelector columnSelector)
    {
      this.columnSelector = columnSelector;
    }

    @Override
    public List<List<NestedPathPart>> getNestedFields()
    {
      final List<List<NestedPathPart>> retVal = new ArrayList<>();
      for (final String keyString : keyExprMap.keySet()) {
        retVal.add(Collections.singletonList(new NestedPathField(keyString)));
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

      final TypedExpression typedExpression = getExpressionForPath(path);
      if (typedExpression != null) {
        return Set.of(typedExpression.type);
      } else {
        return Set.of();
      }
    }

    @Override
    @Nullable
    public ColumnType getFieldLogicalType(List<NestedPathPart> path)
    {
      final Set<ColumnType> fieldTypes = getFieldTypes(path);
      if (fieldTypes == null || fieldTypes.isEmpty()) {
        return null;
      } else {
        return fieldTypes.iterator().next();
      }
    }

    @Override
    public boolean isNumeric(List<NestedPathPart> path)
    {
      final ColumnType type = getFieldLogicalType(path);
      return type != null && type.isNumeric();
    }

    @Override
    public DimensionSelector makeDimensionSelector(
        List<NestedPathPart> path,
        @Nullable ExtractionFn extractionFn,
        ColumnSelectorFactory columnSelectorFactory,
        ReadableOffset readableOffset
    )
    {
      if (path.isEmpty()) {
        return NestedObjectVirtualColumn.this.makeDimensionSelector(
            DefaultDimensionSpec.of(NestedObjectVirtualColumn.this.getOutputName()),
            columnSelectorFactory,
            columnSelector,
            readableOffset
        );
      }

      final TypedExpression typedExpression = getExpressionForPath(path);
      final VirtualColumn vc = getVirtualColumnForExpression(path, typedExpression);
      if (vc == null) {
        return DimensionSelector.constant(null, extractionFn);
      }

      final String key = path.get(0).getPartIdentifier();
      final DimensionSpec dimensionSpec;

      if (extractionFn != null) {
        dimensionSpec = new ExtractionDimensionSpec(key, key, typedExpression.type, extractionFn);
      } else {
        dimensionSpec = DefaultDimensionSpec.of(key, typedExpression.type);
      }

      return vc.makeDimensionSelector(dimensionSpec, columnSelectorFactory, columnSelector, readableOffset);
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(
        List<NestedPathPart> path,
        ColumnSelectorFactory columnSelectorFactory,
        ReadableOffset readableOffset
    )
    {
      if (path.isEmpty()) {
        return NestedObjectVirtualColumn.this.makeColumnValueSelector(
            NestedObjectVirtualColumn.this.getOutputName(),
            columnSelectorFactory,
            columnSelector,
            readableOffset
        );
      }

      final TypedExpression typedExpression = getExpressionForPath(path);
      final VirtualColumn vc = getVirtualColumnForExpression(path, typedExpression);
      if (vc == null) {
        return NilColumnValueSelector.instance();
      }

      final String key = path.get(0).getPartIdentifier();

      return vc.makeColumnValueSelector(
          key,
          columnSelectorFactory,
          columnSelector,
          readableOffset
      );
    }

    @Override
    public SingleValueDimensionVectorSelector makeSingleValueDimensionVectorSelector(
        List<NestedPathPart> path,
        VectorColumnSelectorFactory columnSelectorFactory,
        ReadableVectorOffset readableOffset
    )
    {
      if (path.isEmpty()) {
        return NestedObjectVirtualColumn.this.makeSingleValueVectorDimensionSelector(
            DefaultDimensionSpec.of(NestedObjectVirtualColumn.this.getOutputName()),
            columnSelectorFactory,
            columnSelector,
            readableOffset
        );
      }

      final TypedExpression typedExpression = getExpressionForPath(path);
      final VirtualColumn vc = getVirtualColumnForExpression(path, typedExpression);
      if (vc == null) {
        return NilVectorSelector.create(readableOffset);
      }

      final String key = path.get(0).getPartIdentifier();

      return vc.makeSingleValueVectorDimensionSelector(
          DefaultDimensionSpec.of(key),
          columnSelectorFactory,
          columnSelector,
          readableOffset
      );
    }

    @Override
    public VectorObjectSelector makeVectorObjectSelector(
        List<NestedPathPart> path,
        VectorColumnSelectorFactory columnSelectorFactory,
        ReadableVectorOffset readableOffset
    )
    {
      if (path.isEmpty()) {
        return NestedObjectVirtualColumn.this.makeVectorObjectSelector(
            NestedObjectVirtualColumn.this.getOutputName(),
            columnSelectorFactory,
            columnSelector,
            readableOffset
        );
      }

      final TypedExpression typedExpression = getExpressionForPath(path);
      final VirtualColumn vc = getVirtualColumnForExpression(path, typedExpression);
      if (vc == null) {
        return NilVectorSelector.create(readableOffset);
      }

      final String key = path.get(0).getPartIdentifier();

      return vc.makeVectorObjectSelector(
          key,
          columnSelectorFactory,
          columnSelector,
          readableOffset
      );
    }

    @Override
    public VectorValueSelector makeVectorValueSelector(
        List<NestedPathPart> path,
        VectorColumnSelectorFactory columnSelectorFactory,
        ReadableVectorOffset readableOffset
    )
    {
      if (path.isEmpty()) {
        return NestedObjectVirtualColumn.this.makeVectorValueSelector(
            NestedObjectVirtualColumn.this.getOutputName(),
            columnSelectorFactory,
            columnSelector,
            readableOffset
        );
      }

      final TypedExpression typedExpression = getExpressionForPath(path);
      final VirtualColumn vc = getVirtualColumnForExpression(path, typedExpression);
      if (vc == null) {
        return NilVectorSelector.create(readableOffset);
      }

      final String key = path.get(0).getPartIdentifier();

      return vc.makeVectorValueSelector(
          key,
          columnSelectorFactory,
          columnSelector,
          readableOffset
      );
    }

    @Override
    @Nullable
    public ColumnIndexSupplier getColumnIndexSupplier(List<NestedPathPart> path)
    {
      if (path.isEmpty()) {
        return NestedObjectVirtualColumn.this.getIndexSupplier(
            NestedObjectVirtualColumn.this.getOutputName(),
            columnSelector
        );
      }

      final TypedExpression typedExpression = getExpressionForPath(path);
      final VirtualColumn vc = getVirtualColumnForExpression(path, typedExpression);

      if (vc != null) {
        return vc.getIndexSupplier(vc.getOutputName(), columnSelector);
      } else {
        return null;
      }
    }

    /**
     * Returns the {@link TypedExpression} corresponding to a path. Do not call this method with an empty path;
     * this case should be handled without using {@link TypedExpression}.
     */
    @Nullable
    private TypedExpression getExpressionForPath(final List<NestedPathPart> path)
    {
      if (path.isEmpty()) {
        throw DruidException.defensive("Empty path should be handled by the caller, not here.");
      } else if (path.size() > 1) {
        // AsNestedColumn is only created if all values are primitive; cannot have anything beyond one level.
        return null;
      } else {
        return (TypedExpression) path.get(0).find(keyExprMap);
      }
    }

    /**
     * Returns the {@link VirtualColumn} corresponding to a path. Do not call this method with an empty path;
     * this case should be handled without using {@link TypedExpression}.
     *
     * @param path            the path
     * @param typedExpression result of {@link #getExpressionForPath(List)}
     */
    @Nullable
    private VirtualColumn getVirtualColumnForExpression(
        final List<NestedPathPart> path,
        @Nullable final TypedExpression typedExpression
    )
    {
      if (typedExpression != null) {
        return keyColumnMap.computeIfAbsent(
            path.get(0).getPartIdentifier(),
            fieldName -> new ExpressionVirtualColumn(
                fieldName,
                typedExpression.expression,
                typedExpression.type,
                macroTable
            )
        );
      } else {
        return null;
      }
    }
  }

  /**
   * Represents an expression and a target type. Like a miniature {@link ExpressionVirtualColumn} with no output name.
   */
  public static class TypedExpression
  {
    private final String expression;
    private final ColumnType type;

    @JsonCreator
    public TypedExpression(
        @JsonProperty("expression") String expression,
        @JsonProperty("type") ColumnType type
    )
    {
      this.expression = Preconditions.checkNotNull(expression, "expression");
      this.type = Preconditions.checkNotNull(type, "type");
    }

    @JsonProperty
    public String getExpression()
    {
      return expression;
    }

    @JsonProperty
    public ColumnType getType()
    {
      return type;
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TypedExpression that = (TypedExpression) o;
      return Objects.equals(expression, that.expression) && Objects.equals(type, that.type);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(expression, type);
    }

    @Override
    public String toString()
    {
      return "TypedExpression{" +
             "expression='" + expression + '\'' +
             ", type=" + type +
             '}';
    }
  }
}
