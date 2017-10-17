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

package io.druid.query.aggregation;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import io.druid.math.expr.ExprMacroTable;
import io.druid.math.expr.Parser;
import io.druid.segment.ColumnSelectorFactory;
import io.druid.segment.DoubleColumnSelector;
import io.druid.segment.column.Column;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public abstract class SimpleDoubleAggregatorFactory extends AggregatorFactory
{
  protected final String name;
  protected final String fieldName;
  protected final String expression;
  protected final ExprMacroTable macroTable;
  protected final boolean storeDoubleAsFloat;

  public SimpleDoubleAggregatorFactory(
      ExprMacroTable macroTable,
      String fieldName,
      String name,
      String expression
  )
  {
    this.macroTable = macroTable;
    this.fieldName = fieldName;
    this.name = name;
    this.expression = expression;
    this.storeDoubleAsFloat = Column.storeDoubleAsFloat();
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ expression == null,
        "Must have a valid, non-null fieldName or expression"
    );
  }

  protected DoubleColumnSelector getDoubleColumnSelector(ColumnSelectorFactory metricFactory, Double nullValue)
  {
    return AggregatorUtil.getDoubleColumnSelector(metricFactory, macroTable, fieldName, expression, nullValue);
  }

  @Override
  public Object deserialize(Object object)
  {
    // handle "NaN" / "Infinity" values serialized as strings in JSON
    if (object instanceof String) {
      return Double.parseDouble((String) object);
    }
    return object;
  }

  @Override
  public String getTypeName()
  {
    if (storeDoubleAsFloat) {
      return "float";
    }
    return "double";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Double.BYTES;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(fieldName, expression, name);
  }

  @Override
  public Comparator getComparator()
  {
    return DoubleSumAggregator.COMPARATOR;
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    if (other.getName().equals(this.getName()) && this.getClass() == other.getClass()) {
      return getCombiningFactory();
    } else {
      throw new AggregatorFactoryNotMergeableException(this, other);
    }
  }

  @Override
  public List<String> requiredFields()
  {
    return fieldName != null
           ? Collections.singletonList(fieldName)
           : Parser.findRequiredBindings(Parser.parse(expression, macroTable));
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @JsonProperty
  public String getFieldName()
  {
    return fieldName;
  }

  @JsonProperty
  public String getExpression()
  {
    return expression;
  }

}
