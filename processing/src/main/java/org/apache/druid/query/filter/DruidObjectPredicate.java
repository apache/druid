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

import javax.annotation.Nullable;

public interface DruidObjectPredicate<T>
{
  static <T> DruidObjectPredicate<T> alwaysFalseWithNullUnknown()
  {
    return value -> value == null ? DruidPredicateMatch.UNKNOWN : DruidPredicateMatch.FALSE;
  }

  static <T> DruidObjectPredicate<T> alwaysTrue()
  {
    return value -> DruidPredicateMatch.TRUE;
  }

  static <T> DruidObjectPredicate<T> equalTo(T val)
  {
    return value -> {
      if (value == null) {
        return DruidPredicateMatch.UNKNOWN;
      }
      return DruidPredicateMatch.of(val.equals(value));
    };
  }

  static <T> DruidObjectPredicate<T> notEqualTo(T val)
  {
    return value -> {
      if (value == null) {
        return DruidPredicateMatch.UNKNOWN;
      }
      return DruidPredicateMatch.of(!val.equals(value));
    };
  }

  static <T> DruidObjectPredicate<T> isNull()
  {
    return value -> DruidPredicateMatch.of(value == null);
  }

  static <T> DruidObjectPredicate<T> notNull()
  {
    return value -> DruidPredicateMatch.of(value != null);
  }

  DruidPredicateMatch apply(@Nullable T value);
}
