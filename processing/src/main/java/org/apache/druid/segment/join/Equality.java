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

package org.apache.druid.segment.join;

import org.apache.druid.math.expr.Expr;

/**
 * Represents a join condition between a left-hand-side expression (leftExpr) and a right-hand-side direct
 * column access (rightColumn). This is a particularly interesting kind of condition because it can be resolved
 * using a hashtable on the right-hand side.
 *
 * Note that this class does not override "equals" or "hashCode". This is because Expr also does not.
 */
public class Equality
{
  private final Expr leftExpr;
  private final String rightColumn;

  public Equality(final Expr leftExpr, final String rightColumn)
  {
    this.leftExpr = leftExpr;
    this.rightColumn = rightColumn;
  }

  public Expr getLeftExpr()
  {
    return leftExpr;
  }

  public String getRightColumn()
  {
    return rightColumn;
  }

  @Override
  public String toString()
  {
    return "Equality{" +
           "leftExpr=" + leftExpr +
           ", rightColumn='" + rightColumn + '\'' +
           '}';
  }
}
