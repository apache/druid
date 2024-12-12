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

import org.apache.druid.error.DruidException;

import javax.annotation.Nullable;
import java.util.Objects;

public class StringPredicateDruidPredicateFactory implements DruidPredicateFactory
{
  public static StringPredicateDruidPredicateFactory equalTo(@Nullable String value)
  {
    if (value == null) {
      return new StringPredicateDruidPredicateFactory(DruidObjectPredicate.isNull());
    }
    return new StringPredicateDruidPredicateFactory(DruidObjectPredicate.equalTo(value));
  }

  public static StringPredicateDruidPredicateFactory of(@Nullable DruidObjectPredicate<String> predicate)
  {
    return new StringPredicateDruidPredicateFactory(predicate);
  }

  @Nullable
  private final DruidObjectPredicate<String> predicate;

  private StringPredicateDruidPredicateFactory(DruidObjectPredicate<String> predicate)
  {
    this.predicate = predicate;
  }

  @Override
  public DruidObjectPredicate<String> makeStringPredicate()
  {
    return predicate;
  }

  @Override
  public DruidLongPredicate makeLongPredicate()
  {
    throw DruidException.defensive("String equality predicate factory only supports string predicates");
  }

  @Override
  public DruidFloatPredicate makeFloatPredicate()
  {
    throw DruidException.defensive("String equality predicate factory only supports string predicates");
  }

  @Override
  public DruidDoublePredicate makeDoublePredicate()
  {
    throw DruidException.defensive("String equality predicate factory only supports string predicates");
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    StringPredicateDruidPredicateFactory that = (StringPredicateDruidPredicateFactory) o;
    return Objects.equals(predicate, that.predicate);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(predicate);
  }
}
