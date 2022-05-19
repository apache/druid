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

package org.apache.druid.query.filter;

import com.google.common.base.Predicate;
import org.apache.druid.annotations.SubclassesMustOverrideEqualsAndHashCode;

@SubclassesMustOverrideEqualsAndHashCode
public interface DruidPredicateFactory
{
  Predicate<String> makeStringPredicate();

  DruidLongPredicate makeLongPredicate();

  DruidFloatPredicate makeFloatPredicate();

  DruidDoublePredicate makeDoublePredicate();

  /**
   * Object predicate is currently only used by vectorized matchers for non-string object selectors. This currently
   * means it will be used only if we encounter COMPLEX types, but will also include array types once they are more
   * supported throughout the query engines.
   *
   * To preserve behavior with non-vectorized matchers which use a string predicate with null inputs for these 'nil'
   * matchers, we do the same thing here.
   *
   * @see org.apache.druid.segment.VectorColumnProcessorFactory#makeObjectProcessor
   */
  default Predicate<Object> makeObjectPredicate()
  {
    final Predicate<String> stringPredicate = makeStringPredicate();
    return o -> stringPredicate.apply(null);
  }
}
