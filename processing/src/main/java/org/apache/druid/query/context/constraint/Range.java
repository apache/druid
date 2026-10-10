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

package org.apache.druid.query.context.constraint;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.BadQueryContextException;

import java.util.Objects;

/**
 * Factory for range-based parameter constraints.
 */
public final class Range
{
  /**
   * A range constraint whose bounds are available to documentation generators.
   */
  public interface Constraint<T> extends ParameterConstraint<T>
  {
    T getLowerBound();

    T getUpperBound();
  }

  private Range()
  {
  }

  /**
   * Creates an inclusive range using the natural ordering of its bounds and values.
   */
  public static <T extends Comparable<? super T>> Constraint<T> closedRange(
      final T lowerBound,
      final T upperBound
  )
  {
    Objects.requireNonNull(lowerBound, "lowerBound");
    Objects.requireNonNull(upperBound, "upperBound");
    if (lowerBound.compareTo(upperBound) > 0) {
      throw new IAE("Closed range lower bound [%s] must not exceed upper bound [%s]", lowerBound, upperBound);
    }

    return new Constraint<>()
    {
      @Override
      public T getLowerBound()
      {
        return lowerBound;
      }

      @Override
      public T getUpperBound()
      {
        return upperBound;
      }

      @Override
      public void validate(final String parameterName, final T value)
      {
        if (value.compareTo(lowerBound) < 0 || value.compareTo(upperBound) > 0) {
          throw outOfRangeException(parameterName, lowerBound, upperBound, value);
        }
      }
    };
  }

  /**
   * Creates the exception reported when a value falls outside an inclusive range. The value may be of a wider type
   * than the bounds, for example a number that does not fit the parameter's value type.
   */
  public static BadQueryContextException outOfRangeException(
      final String parameterName,
      final Object lowerBound,
      final Object upperBound,
      final Object value
  )
  {
    return new BadQueryContextException(
        StringUtils.format(
            "Query context parameter [%s] must be within the range [%s, %s], but was [%s]",
            parameterName,
            lowerBound,
            upperBound,
            value
        )
    );
  }
}
