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

import org.apache.druid.math.expr.Evals;

public class VectorComparisonProcessors
{
  public static BivariateFunctionVectorProcessorFactory equals()
  {
    return Equal.INSTANCE;
  }

  public static BivariateFunctionVectorProcessorFactory notEquals()
  {
    return NotEqual.INSTANCE;
  }

  public static BivariateFunctionVectorProcessorFactory greaterThanOrEquals()
  {
    return GreaterThanOrEqual.INSTANCE;
  }

  public static BivariateFunctionVectorProcessorFactory greaterThan()
  {
    return GreaterThan.INSTANCE;
  }

  public static BivariateFunctionVectorProcessorFactory lessThanOrEquals()
  {
    return LessThanOrEqual.INSTANCE;
  }

  public static BivariateFunctionVectorProcessorFactory lessThan()
  {
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

  private VectorComparisonProcessors()
  {
    // No instantiation
  }
}
