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

package org.apache.druid.delta.filter;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.delta.kernel.expressions.Predicate;
import io.delta.kernel.types.StructType;

/**
 * Druid filters that translate to the underlying Delta Kernel {@link Predicate}s. Implementations should
 * provide an expression tree syntax to provide more flexibility to users.
 *
 * <p>
 * A user-facing Druid {@link DeltaFilter} should be translated to a canonical Delta Kernel {@link Predicate}.
 * Implementations should provide this one-to-one translation.
 * </p>
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "=", value = DeltaEqualsFilter.class),
    @JsonSubTypes.Type(name = ">", value = DeltaGreaterThanFilter.class),
    @JsonSubTypes.Type(name = ">=", value = DeltaGreaterThanOrEqualsFilter.class),
    @JsonSubTypes.Type(name = "<", value = DeltaLessThanFilter.class),
    @JsonSubTypes.Type(name = "<=", value = DeltaLessThanOrEqualsFilter.class),
    @JsonSubTypes.Type(name = "and", value = DeltaAndFilter.class),
    @JsonSubTypes.Type(name = "or", value = DeltaOrFilter.class),
    @JsonSubTypes.Type(name = "not", value = DeltaNotFilter.class),
})
public interface DeltaFilter
{
  /**
   * Return a Delta predicate expression. The {@code snapshotSchema} should be used to perform any validations
   * and derive sub-expressions to be used in the resulting {@link Predicate}.
   */
  Predicate getFilterPredicate(StructType snapshotSchema);
}
