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


import com.google.common.base.Preconditions;
import io.druid.math.expr.ExprMacroTable;
import io.druid.math.expr.Parser;
import io.druid.segment.BaseFloatColumnValueSelector;
import io.druid.segment.ColumnSelectorFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public abstract class SimpleFloatAggregatorFactory extends AggregatorFactory
{
  protected final String name;
  protected final String fieldName;
  protected final String expression;
  protected final ExprMacroTable macroTable;

  public SimpleFloatAggregatorFactory(
      ExprMacroTable macroTable,
      String name,
      final String fieldName,
      String expression
  )
  {
    this.macroTable = macroTable;
    this.name = name;
    this.fieldName = fieldName;
    this.expression = expression;
    Preconditions.checkNotNull(name, "Must have a valid, non-null aggregator name");
    Preconditions.checkArgument(
        fieldName == null ^ expression == null,
        "Must have a valid, non-null fieldName or expression"
    );
  }

  BaseFloatColumnValueSelector makeColumnValueSelectorWithFloatDefault(
      ColumnSelectorFactory metricFactory,
      float nullValue
  )
  {
    return AggregatorUtil.makeColumnValueSelectorWithFloatDefault(
        metricFactory,
        macroTable,
        fieldName,
        expression,
        nullValue
    );
  }

  @Override
  public Object deserialize(Object object)
  {
    // handle "NaN" / "Infinity" values serialized as strings in JSON
    if (object instanceof String) {
      return Float.parseFloat((String) object);
    }
    return object;
  }

  @Override
  public String getTypeName()
  {
    return "float";
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return Float.BYTES;
  }

  @Override
  public Comparator getComparator()
  {
    return FloatSumAggregator.COMPARATOR;
  }

  @Override
  public Object finalizeComputation(Object object)
  {
    return object;
  }

  @Override
  public List<String> requiredFields()
  {
    return fieldName != null
           ? Collections.singletonList(fieldName)
           : Parser.findRequiredBindings(Parser.parse(expression, macroTable));
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
  public int hashCode()
  {
    return Objects.hash(fieldName, expression, name);
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

    SimpleFloatAggregatorFactory that = (SimpleFloatAggregatorFactory) o;

    if (!Objects.equals(fieldName, that.fieldName)) {
      return false;
    }
    if (!Objects.equals(expression, that.expression)) {
      return false;
    }
    if (!Objects.equals(name, that.name)) {
      return false;
    }
    return true;
  }
}
