/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.query.groupby.having;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.data.input.Row;

/**
 * A "having" clause that filters aggregated value. This is similar to SQL's "having"
 * clause.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = AlwaysHavingSpec.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "and", value = AndHavingSpec.class),
    @JsonSubTypes.Type(name = "or", value = OrHavingSpec.class),
    @JsonSubTypes.Type(name = "not", value = NotHavingSpec.class),
    @JsonSubTypes.Type(name = "greaterThan", value = GreaterThanHavingSpec.class),
    @JsonSubTypes.Type(name = "lessThan", value = LessThanHavingSpec.class),
    @JsonSubTypes.Type(name = "equalTo", value = EqualToHavingSpec.class)
})
public interface HavingSpec
{
  // Atoms for easy combination, but for now they are mostly useful
  // for testing.
  public static final HavingSpec NEVER = new NeverHavingSpec();
  public static final HavingSpec ALWAYS = new AlwaysHavingSpec();

  /**
   * Evaluates if a given row satisfies the having spec.
   *
   * @param row A Row of data that may contain aggregated values
   *
   * @return true if the given row satisfies the having spec. False otherwise.
   */
  public boolean eval(Row row);

  public byte[] getCacheKey();
}
