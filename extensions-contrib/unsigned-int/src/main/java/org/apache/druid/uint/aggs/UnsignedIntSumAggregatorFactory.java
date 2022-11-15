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

package org.apache.druid.uint.aggs;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.uint.UnsignedIntComplexSerde;

import javax.annotation.Nullable;

public class UnsignedIntSumAggregatorFactory extends LongSumAggregatorFactory
{

  public static final ColumnType TYPE = ColumnType.ofComplex(UnsignedIntComplexSerde.TYPE);

  @JsonCreator
  public UnsignedIntSumAggregatorFactory(
      @JsonProperty("name") String name,
      @JsonProperty("fieldName") final String fieldName,
      @JsonProperty("expression") @Nullable String expression,
      @JacksonInject ExprMacroTable macroTable
  )
  {
    super(name, fieldName, expression, macroTable);
  }

  @Override
  public ColumnType getIntermediateType()
  {
    return TYPE;
  }

}
