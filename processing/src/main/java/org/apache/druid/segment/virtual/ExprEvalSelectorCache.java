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
import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.segment.ColumnValueSelector;

/**
 * Supplies expression selectors cached for the lifetime of an ingestion column selector factory.
 *
 * Implementations may return one mutable selector shared across callers. Callers must only use returned selectors from
 * the thread that ingests rows. Query-time column selector factories, which may be used from multiple threads, must not
 * implement this interface.
 */
public interface ExprEvalSelectorCache
{
  /**
   * Returns a cached or newly created selector for an expression.
   *
   * @param expression expression to evaluate
   * @return cached or newly created selector
   */
  ColumnValueSelector<ExprEval> getOrCreateExprEvalSelector(Expr expression);
}
