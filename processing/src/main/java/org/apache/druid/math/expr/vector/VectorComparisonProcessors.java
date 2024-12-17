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

package org.apache.druid.math.expr.vector;

import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.math.expr.Evals;
import org.apache.druid.math.expr.ExpressionProcessing;

import java.util.Objects;

public class VectorComparisonProcessors
{
  public static BivariateFunctionVectorProcessorFactory equals()
  {
    if (!ExpressionProcessing.useStrictBooleans()) {
      return LegacyEqual.INSTANCE;
    }
    return Equal.INSTANCE;
  }

  public static BivariateFunctionVectorProcessorFactory notEquals()
  {
    if (!ExpressionProcessing.useStrictBooleans()) {
      return LegacyNotEqual.INSTANCE;
    }
    return NotEqual.INSTANCE;
  }

  public static BivariateFunctionVectorProcessorFactory greaterThanOrEquals()
  {
    if (!ExpressionProcessing.useStrictBooleans()) {
      return LegacyGreaterThanOrEqual.INSTANCE;
    }
    return GreaterThanOrEqual.INSTANCE;
  }

  public static BivariateFunctionVectorProcessorFactory greaterThan()
  {
    if (!ExpressionProcessing.useStrictBooleans()) {
      return LegacyGreaterThan.INSTANCE;
    }
    return GreaterThan.INSTANCE;
  }

  public static BivariateFunctionVectorProcessorFactory lessThanOrEquals()
  {
    if (!ExpressionProcessing.useStrictBooleans()) {
      return LegacyLessThanOrEqual.INSTANCE;
    }
    return LessThanOrEqual.INSTANCE;
  }

  public static BivariateFunctionVectorProcessorFactory lessThan()
  {
    if (!ExpressionProcessing.useStrictBooleans()) {
      return LegacyLessThan.INSTANCE;
    }
    return LessThan.INSTANCE;
  }

  public static class Equal extends SimpleVectorComparisonProcessorFactory
  {
    private static final Equal INSTANCE = new Equal();

    public Equal()
    {
      super(
          (compareResult) -> compareResult == 0,
          (left, right) -> Evals.asLong(left == right),
          (left, right) -> Evals.asLong(left == right),
          (left, right) -> Evals.asLong(left == right),
          (left, right) -> Evals.asLong(left == right)
      );
    }
  }

  public static class NotEqual extends SimpleVectorComparisonProcessorFactory
  {
    private static final NotEqual INSTANCE = new NotEqual();

    public NotEqual()
    {
      super(
          compareResult -> compareResult != 0,
          (left, right) -> Evals.asLong(left != right),
          (left, right) -> Evals.asLong(left != right),
          (left, right) -> Evals.asLong(left != right),
          (left, right) -> Evals.asLong(left != right)
      );
    }
  }

  public static class GreaterThanOrEqual extends SimpleVectorComparisonProcessorFactory
  {
    private static final GreaterThanOrEqual INSTANCE = new GreaterThanOrEqual();

    public GreaterThanOrEqual()
    {
      super(
          compareResult -> compareResult >= 0,
          (left, right) -> Evals.asLong(left >= right),
          (left, right) -> Evals.asLong(left >= right),
          (left, right) -> Evals.asLong(left >= right),
          (left, right) -> Evals.asLong(left >= right)
      );
    }
  }

  public static class GreaterThan extends SimpleVectorComparisonProcessorFactory
  {
    private static final GreaterThan INSTANCE = new GreaterThan();

    public GreaterThan()
    {
      super(
          compareResult -> compareResult > 0,
          (left, right) -> Evals.asLong(left > right),
          (left, right) -> Evals.asLong(left > right),
          (left, right) -> Evals.asLong(left > right),
          (left, right) -> Evals.asLong(left > right)
      );
    }
  }

  public static class LessThanOrEqual extends SimpleVectorComparisonProcessorFactory
  {
    private static final LessThanOrEqual INSTANCE = new LessThanOrEqual();

    public LessThanOrEqual()
    {
      super(
          compareResult -> compareResult <= 0,
          (left, right) -> Evals.asLong(left <= right),
          (left, right) -> Evals.asLong(left <= right),
          (left, right) -> Evals.asLong(left <= right),
          (left, right) -> Evals.asLong(left <= right)
      );
    }
  }

  public static class LessThan extends SimpleVectorComparisonProcessorFactory
  {
    private static final LessThan INSTANCE = new LessThan();

    public LessThan()
    {
      super(
          compareResult -> compareResult < 0,
          (left, right) -> Evals.asLong(left < right),
          (left, right) -> Evals.asLong(left < right),
          (left, right) -> Evals.asLong(left < right),
          (left, right) -> Evals.asLong(left < right)
      );
    }
  }

  public static class LegacyEqual extends SimpleVectorComparisonLegacyProcessorFactory
  {
    private static final LegacyEqual INSTANCE = new LegacyEqual();

    public LegacyEqual()
    {
      super(
          (left, right) -> Evals.asLong(Objects.equals(left, right)),
          (left, right) -> Evals.asLong(left == right),
          (left, right) -> Evals.asDouble(left == right),
          (left, right) -> Evals.asDouble(left == right),
          (left, right) -> Evals.asDouble(left == right)
      );
    }
  }

  public static class LegacyNotEqual extends SimpleVectorComparisonLegacyProcessorFactory
  {
    private static final LegacyNotEqual INSTANCE = new LegacyNotEqual();

    public LegacyNotEqual()
    {
      super(
          (left, right) -> Evals.asLong(!Objects.equals(left, right)),
          (left, right) -> Evals.asLong(left != right),
          (left, right) -> Evals.asDouble(left != right),
          (left, right) -> Evals.asDouble(left != right),
          (left, right) -> Evals.asDouble(left != right)
      );
    }
  }

  public static class LegacyGreaterThanOrEqual extends SimpleVectorComparisonLegacyProcessorFactory
  {
    private static final LegacyGreaterThanOrEqual INSTANCE = new LegacyGreaterThanOrEqual();

    public LegacyGreaterThanOrEqual()
    {
      super(
          (left, right) -> Evals.asLong(
              Comparators.<String>naturalNullsFirst().compare((String) left, (String) right) >= 0
          ),
          (left, right) -> Evals.asLong(left >= right),
          (left, right) -> Evals.asDouble(left >= right),
          (left, right) -> Evals.asDouble(left >= right),
          (left, right) -> Evals.asDouble(left >= right)
      );
    }
  }

  public static class LegacyGreaterThan extends SimpleVectorComparisonLegacyProcessorFactory
  {
    private static final LegacyGreaterThan INSTANCE = new LegacyGreaterThan();

    public LegacyGreaterThan()
    {
      super(
          (left, right) -> Evals.asLong(
              Comparators.<String>naturalNullsFirst().compare((String) left, (String) right) > 0
          ),
          (left, right) -> Evals.asLong(left > right),
          (left, right) -> Evals.asDouble(left > right),
          (left, right) -> Evals.asDouble(left > right),
          (left, right) -> Evals.asDouble(left > right)
      );
    }
  }

  public static class LegacyLessThanOrEqual extends SimpleVectorComparisonLegacyProcessorFactory
  {
    private static final LegacyLessThanOrEqual INSTANCE = new LegacyLessThanOrEqual();

    public LegacyLessThanOrEqual()
    {
      super(
          (left, right) -> Evals.asLong(
              Comparators.<String>naturalNullsFirst().compare((String) left, (String) right) <= 0
          ),
          (left, right) -> Evals.asLong(left <= right),
          (left, right) -> Evals.asDouble(left <= right),
          (left, right) -> Evals.asDouble(left <= right),
          (left, right) -> Evals.asDouble(left <= right)
      );
    }
  }

  public static class LegacyLessThan extends SimpleVectorComparisonLegacyProcessorFactory
  {
    private static final LegacyLessThan INSTANCE = new LegacyLessThan();

    public LegacyLessThan()
    {
      super(
          (left, right) -> Evals.asLong(
              Comparators.<String>naturalNullsFirst().compare((String) left, (String) right) < 0
          ),
          (left, right) -> Evals.asLong(left < right),
          (left, right) -> Evals.asDouble(left < right),
          (left, right) -> Evals.asDouble(left < right),
          (left, right) -> Evals.asDouble(left < right)
      );
    }
  }

  private VectorComparisonProcessors()
  {
    // No instantiation
  }
}
