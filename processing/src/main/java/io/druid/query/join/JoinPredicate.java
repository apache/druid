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

package io.druid.query.join;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

@JsonTypeInfo(use = Id.NAME, property = "type")
@JsonSubTypes(value = {
    @Type(name = "and", value = AndPredicate.class),
    @Type(name = "or", value = OrPredicate.class),
    @Type(name = "not", value = NotPredicate.class),
    @Type(name = "equal", value = EqualPredicate.class),
    @Type(name = "dimension", value = DimExtractPredicate.class),
    @Type(name = "add", value = AddPredicate.class),
    @Type(name = "subtract", value = SubtractPredicate.class),
    @Type(name = "multiply", value = MultiplyPredicate.class),
    @Type(name = "divide", value = DividePredicate.class),
    @Type(name = "literal", value = LiteralPredicate.class)
})
public interface JoinPredicate
{
  enum PredicateType {
    AND,
    OR,
    NOT,
    EQUAL,
    DIMENSION,
    ADD,
    SUBTRACT,
    MULTIPLY,
    DIVIDE,
    LITERAL
  }

  PredicateType getType();

  void accept(JoinPredicateVisitor visitor);
}
