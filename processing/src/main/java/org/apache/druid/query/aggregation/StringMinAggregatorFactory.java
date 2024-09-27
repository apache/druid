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

package org.apache.druid.query.aggregation;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.BaseObjectColumnValueSelector;

import javax.annotation.Nullable;
import java.util.Comparator;

public class StringMinAggregatorFactory extends SimpleStringAggregatorFactory
{
  private static final Comparator<String> VALUE_COMPARATOR = Comparator.nullsLast(Comparator.naturalOrder());

  private final boolean aggregateMultipleValues;
  private final Supplier<byte[]> cacheKey;

  @JsonCreator
  public StringMinAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") @Nullable String fieldName,
      @JsonProperty("maxStringBytes") @Nullable Integer maxStringBytes,
      @JsonProperty("aggregateMultipleValues") @Nullable final Boolean aggregateMultipleValues,
      @JsonProperty("expression") @Nullable String expression,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    super(macroTable, name, fieldName, expression, maxStringBytes);

    this.aggregateMultipleValues = aggregateMultipleValues == null || aggregateMultipleValues;
    this.cacheKey = AggregatorUtil.getSimpleAggregatorCacheKeySupplier(
        AggregatorUtil.STRING_MIN_CACHE_TYPE_ID,
        fieldName,
        fieldExpression
    );
  }

  public StringMinAggregatorFactory(
      String name,
      String fieldName,
      Integer maxStringBytes,
      boolean aggregateMultipleValues
  )
  {
    this(name, fieldName, maxStringBytes, aggregateMultipleValues, null, ExprMacroTable.nil());
  }

  @Override
  protected Aggregator buildAggregator(BaseObjectColumnValueSelector<String> selector)
  {
    return new StringMinAggregator(selector, maxStringBytes);
  }

  @Override
  protected BufferAggregator buildBufferAggregator(BaseObjectColumnValueSelector<String> selector)
  {
    return new StringMinBufferAggregator(selector, maxStringBytes);
  }

  @Override
  public Comparator<String> getComparator()
  {
    return StringMinAggregatorFactory.VALUE_COMPARATOR;
  }

  @Nullable
  @Override
  public Object combine(@Nullable Object lhs, @Nullable Object rhs)
  {
    return StringMinAggregator.combineValues((String) lhs, (String) rhs);
  }

  @JsonIgnore
  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return new StringMinAggregatorFactory(name, name, maxStringBytes, aggregateMultipleValues);
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
  public String getName()
  {
    return name;
  }

  @Override
  public AggregatorFactory withName(String newName)
  {
    return new StringMinAggregatorFactory(
        newName,
        fieldName,
        maxStringBytes,
        aggregateMultipleValues,
        expression,
        macroTable
    );
  }

  @Override
  public byte[] getCacheKey()
  {
    return cacheKey.get();
  }


  @Override
  public String toString()
  {
    return "StringMinAggregatorFactory{" +
           "name='" + name + '\'' +
           ", fieldName='" + fieldName + '\'' +
           ", maxStringBytes=" + maxStringBytes +
           ", aggregateMultipleValues=" + aggregateMultipleValues +
           '}';
  }
}
