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

package io.druid.segment.virtual;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.primitives.Ints;
import io.druid.math.expr.Expr;
import io.druid.math.expr.Parser;
import io.druid.query.dimension.DimensionSpec;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DimensionSelector;
import io.druid.segment.FloatColumnSelector;
import io.druid.segment.LongColumnSelector;
import io.druid.segment.ObjectColumnSelector;
import io.druid.segment.VirtualColumn;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ColumnCapabilitiesImpl;
import io.druid.segment.column.ValueType;
import org.apache.commons.codec.Charsets;

import java.nio.ByteBuffer;
import java.util.List;

public class ExpressionVirtualColumn implements VirtualColumn
{
  private static final ColumnCapabilities CAPABILITIES = new ColumnCapabilitiesImpl().setType(ValueType.FLOAT);

  private final String name;
  private final String expression;
  private final Expr parsedExpression;

  @JsonCreator
  public ExpressionVirtualColumn(
      @JsonProperty("name") String name,
      @JsonProperty("expression") String expression
  )
  {
    this.name = Preconditions.checkNotNull(name, "name");
    this.expression = Preconditions.checkNotNull(expression, "expression");
    this.parsedExpression = Parser.parse(expression);
  }

  @JsonProperty("name")
  @Override
  public String getOutputName()
  {
    return name;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

  @Override
  public ObjectColumnSelector makeObjectColumnSelector(
      final String columnName,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    return ExpressionSelectors.makeObjectColumnSelector(columnSelectorFactory, parsedExpression);
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      final DimensionSpec dimensionSpec,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    return dimensionSpec.decorate(
        ExpressionSelectors.makeDimensionSelector(
            columnSelectorFactory,
            parsedExpression,
            dimensionSpec.getExtractionFn()
        )
    );
  }

  @Override
  public FloatColumnSelector makeFloatColumnSelector(
      final String columnName,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    return ExpressionSelectors.makeFloatColumnSelector(columnSelectorFactory, parsedExpression, 0.0f);
  }

  @Override
  public LongColumnSelector makeLongColumnSelector(
      final String columnName,
      final ColumnSelectorFactory columnSelectorFactory
  )
  {
    return ExpressionSelectors.makeLongColumnSelector(columnSelectorFactory, parsedExpression, 0L);
  }

  @Override
  public ColumnCapabilities capabilities(String columnName)
  {
    return CAPABILITIES;
  }

  @Override
  public List<String> requiredColumns()
  {
    return Parser.findRequiredBindings(expression);
  }

  @Override
  public boolean usesDotNotation()
  {
    return false;
  }

  @Override
  public byte[] getCacheKey()
  {
    final byte[] nameBytes = name.getBytes(Charsets.UTF_8);
    final byte[] expressionBytes = expression.getBytes(Charsets.UTF_8);

    return ByteBuffer
        .allocate(1 + Ints.BYTES * 2 + nameBytes.length + expressionBytes.length)
        .put(VirtualColumnCacheHelper.CACHE_TYPE_ID_EXPRESSION)
        .putInt(nameBytes.length)
        .put(nameBytes)
        .putInt(expressionBytes.length)
        .put(expressionBytes)
        .array();
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    ExpressionVirtualColumn that = (ExpressionVirtualColumn) o;

    if (!name.equals(that.name)) {
      return false;
    }
    return expression.equals(that.expression);
  }

  @Override
  public int hashCode()
  {
    int result = name.hashCode();
    result = 31 * result + expression.hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "ExpressionVirtualColumn{" +
           "name='" + name + '\'' +
           ", expression='" + expression + '\'' +
           '}';
  }
}
