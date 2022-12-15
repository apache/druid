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

package org.apache.druid.query.operator;

import org.apache.druid.query.rowsandcols.RowsAndColumns;

/**
 * An Operator interface that intends to align closely with the Operators that other databases would also tend
 * to be implemented using.
 * <p>
 * The lifecycle of an operator is that, after creation, it should be opened, and then iterated using hasNext() and
 * next().  Finally, when the Operator is no longer useful, it should be closed.
 * <p>
 * Operator's methods mimic the methods of an {@code Iterator}, but it does not implement {@code Iterator}
 * intentionally.  An operator should never be wrapped in an {@code Iterator}.  Any code that does that should be
 * considered a bug and fixed.  This is for two reasons:
 * <p>
 * 1. An Operator should never be passed around as an {@code Iterator}.  An Operator must be closed, if an operator
 * gets returned as an {@code Iterator}, the code that sees the {@code Iterator} loses the knowledge that it's
 * dealing with an Operator and might not close it.  Even something like a {@code CloseableIterator} is an
 * anti-pattern as it's possible to use it in a functional manner with code that loses track of the fact that it
 * must be closed.
 * 2. To avoid "fluent" style composition of functions on Operators.  It is important that there never be a set of
 * functional primitives for things like map/filter/reduce to "simplify" the implementation of Operators.  This is
 * because such fluency produces really hard to decipher stack traces as the stacktrace ends up being just a bunch
 * of calls from the scaffolding (map/filter/reduce) and not from the actual Operator itself.  By not implementing
 * {@code Iterator} we are actively increasing the burden of trying to add such functional operations to the point
 * that hopefully, though code review, we can ensure that we never develop them.  It is infinitely better to preserve
 * the stacktrace and "duplicate" the map/filter/reduce scaffolding code.
 */
public interface Operator
{
  /**
   * Called to initiate the lifecycle of the Operator.  If an operator needs to checkout resources or anything to do
   * its work, this is probably the place to do it.
   *
   * Work should *never* be done in this method, this method only exists to acquire resources that are known to be
   * needed before doing any work.  As a litmus test, if there is ever a call to `op.next()` inside of this method,
   * then something has been done wrong as that call to `.next()` is actually doing work.  Such code should be moved
   * into being lazily evaluated as part of a call to `.next()`.
   */
  void open();

  /**
   * Returns the next RowsAndColumns object that the Operator can produce.  Behavior is undefined if
   * {@link #hasNext} returns false.
   *
   * @return the next RowsAndColumns object that the operator can produce
   */
  RowsAndColumns next();

  /**
   * Used to identify if it is safe to call {@link #next}
   *
   * @return true if it is safe to call {@link #next}
   */
  boolean hasNext();

  /**
   * Closes this Operator.  The cascade flag can be used to identify that the intent is to close this operator
   * and only this operator without actually closing child operators.  Other databases us this sort of functionality
   * with a planner that is watching over all of the objects and force-closes even if they were closed during normal
   * operations.  In Druid, in the data pipeline where this was introduced, we are guaranteed to always have close
   * called regardless of errors or exceptions during processing, as such, at time of introduction, there is no
   * call that passes false for cascade.
   * <p>
   * That said, given that this is a common thing for these interfaces for other databases, we want to preserve the
   * optionality of being able to leverage what they do.  As such, we define the method this way with the belief
   * that it might be used in the future.  Semantically, this means that all implementations of Operators must
   * expect to be closed multiple times.  I.e. after being closed, it is an error for open, next or hasNext to be
   * called, but close can be called any number of times.
   *
   * @param cascade whether to call close on child operators.
   */
  void close(boolean cascade);
}
