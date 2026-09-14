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

package org.apache.druid.segment.virtual;

import org.apache.druid.math.expr.Expr;

/**
 * Supplies expression plans cached for the lifetime of a column selector factory.
 *
 * A plan can only be reused while the column capabilities exposed by the selector factory are stable. Implementations
 * may use expression identity as the cache key, so callers should reuse the same parsed expression when possible.
 * Implementations must not share mutable expression selectors or aggregators through this cache.
 */
public interface ExpressionPlanCache
{
  /**
   * Returns the immutable plan for an expression.
   *
   * @param expression expression to plan
   * @return cached or newly created plan
   */
  ExpressionPlan getExpressionPlan(Expr expression);
}
