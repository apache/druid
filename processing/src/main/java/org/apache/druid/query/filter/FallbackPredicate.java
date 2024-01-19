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
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;

import javax.annotation.Nullable;

/**
 * {@link Predicate} that wraps another predicate and can catch {@link ClassCastException} from some
 * {@link java.util.Comparator} and degrades into casting input to an expected {@link ExpressionType} once an exception
 * is encountered. Useful when processing data that might be mixed types, despite what the column capabilities might
 * claim the type is, such as variant 'auto' types. This class is not thread-safe.
 */
public class FallbackPredicate<T> implements DruidObjectPredicate<T>
{
  private final DruidObjectPredicate<T> delegate;
  private final ExpressionType expectedType;
  private boolean needsCast = false;

  public FallbackPredicate(DruidObjectPredicate<T> delegate, ExpressionType expectedType)
  {
    this.delegate = delegate;
    this.expectedType = expectedType;
  }

  @Override
  public DruidPredicateMatch apply(@Nullable T input)
  {
    if (needsCast) {
      return castApply(input);
    }
    try {
      return delegate.apply(input);
    }
    catch (ClassCastException caster) {
      needsCast = true;
      return castApply(input);
    }
  }

  private DruidPredicateMatch castApply(@Nullable T input)
  {
    final ExprEval<T> castEval = ExprEval.bestEffortOf(input).castTo(expectedType);
    return delegate.apply(castEval.value());
  }
}
