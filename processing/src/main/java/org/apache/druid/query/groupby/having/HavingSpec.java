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

package org.apache.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.Cacheable;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ValueType;

import java.util.Map;

/**
 * A "having" clause that filters aggregated/dimension value. This is similar to SQL's "having"
 * clause. HavingSpec objects are *not* thread-safe and must not be used simultaneously by multiple
 * threads.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "and", value = AndHavingSpec.class),
    @JsonSubTypes.Type(name = "or", value = OrHavingSpec.class),
    @JsonSubTypes.Type(name = "not", value = NotHavingSpec.class),
    @JsonSubTypes.Type(name = "greaterThan", value = GreaterThanHavingSpec.class),
    @JsonSubTypes.Type(name = "lessThan", value = LessThanHavingSpec.class),
    @JsonSubTypes.Type(name = "equalTo", value = EqualToHavingSpec.class),
    @JsonSubTypes.Type(name = "dimSelector", value = DimensionSelectorHavingSpec.class),
    @JsonSubTypes.Type(name = "always", value = AlwaysHavingSpec.class),
    @JsonSubTypes.Type(name = "filter", value = DimFilterHavingSpec.class)
})
public interface HavingSpec extends Cacheable
{
  // Atoms for easy combination, but for now they are mostly useful
  // for testing.
  HavingSpec NEVER = new NeverHavingSpec();
  HavingSpec ALWAYS = new AlwaysHavingSpec();

  /**
   * Informs this HavingSpec that rows passed to "eval" will have a certain signature. Will be called
   * before "eval".
   *
   * @param rowSignature signature of the rows
   */
  void setRowSignature(Map<String, ValueType> rowSignature);

  void setAggregators(Map<String, AggregatorFactory> aggregators);

  /**
   * Evaluates if a given row satisfies the having spec.
   *
   * @param row A Row of data that may contain aggregated values
   *
   * @return true if the given row satisfies the having spec. False otherwise.
   */
  boolean eval(Row row);
}
