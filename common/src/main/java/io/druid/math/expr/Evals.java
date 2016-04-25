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

package io.druid.math.expr;

/**
 */
public class Evals
{
  static Number eq(Number leftVal, Number rightVal)
  {
    if (isAnyLong(leftVal, rightVal)) {
      return booleanToLong(leftVal.longValue() == rightVal.longValue());
    } else {
      return booleanToDouble(leftVal.doubleValue() == rightVal.doubleValue());
    }
  }

  static Number booleanToLong(boolean value)
  {
    return value ? 1L : 0L;
  }

  static Number booleanToDouble(boolean value)
  {
    return value ? 1.0d : 0.0d;
  }

  static boolean isAnyLong(Number left, Number right)
  {
    return left instanceof Long && right instanceof Long;
  }
}
