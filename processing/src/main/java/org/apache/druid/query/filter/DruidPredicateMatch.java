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

/**
 * Three-value logic result for matching values with predicates produced by {@link DruidPredicateFactory}
 *
 * @see DruidObjectPredicate
 * @see DruidLongPredicate
 * @see DruidFloatPredicate
 * @see DruidDoublePredicate
 */
public enum DruidPredicateMatch
{
  /**
   * Value does not match
   */
  FALSE,
  /**
   * Value matches
   */
  TRUE,
  /**
   * Value is unknown to match, for example from a null input to a predicate which does not match null as true or false
   */
  UNKNOWN;

  /**
   * Convenience method for {@link ValueMatcher} and {@link org.apache.druid.query.filter.vector.VectorValueMatcher}
   * implementations to pass through the 'includeUnknown' parameter to the predicate match result.
   */
  public boolean matches(boolean includeUnknown)
  {
    return this == TRUE || (includeUnknown && this == UNKNOWN);
  }

  public static DruidPredicateMatch of(boolean val)
  {
    if (val) {
      return TRUE;
    }
    return FALSE;
  }
}
