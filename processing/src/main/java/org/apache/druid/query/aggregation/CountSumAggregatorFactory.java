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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import org.apache.druid.math.expr.ExprMacroTable;

import javax.annotation.Nullable;

public class CountSumAggregatorFactory extends LongSumAggregatorFactory
{

  private final Supplier<byte[]> cacheKey;

  @JsonCreator
  public CountSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("expression") @Nullable String expression,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    super(name, fieldName, expression, macroTable);
    this.cacheKey = AggregatorUtil.getSimpleAggregatorCacheKeySupplier(
        AggregatorUtil.LONG_SUM_CACHE_TYPE_ID,
        fieldName,
        fieldExpression
    );
  }

  public CountSumAggregatorFactory(String name, String fieldName)
  {
    this(name, fieldName, null, ExprMacroTable.nil());
  }

  protected boolean canHandleNulls()
  {
    return true;
  }
}
