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

import org.apache.druid.segment.ColumnSelectorFactory;

/**
 * An interface returned by {@link Joinable#makeJoinMatcher} and used by {@link HashJoinEngine} to implement a join.
 *
 * A typical usage would go something like:
 *
 * <pre>
 * matcher.matchCondition();
 * while (matcher.hasMatch()) {
 *   // Do something with the match
 *   matcher.nextMatch();
 * }
 * </pre>
 */
public interface JoinMatcher
{
  /**
   * Returns a factory for reading columns from the {@link Joinable} that correspond to matched rows.
   */
  ColumnSelectorFactory getColumnSelectorFactory();

  /**
   * Matches against the {@link ColumnSelectorFactory} and {@link JoinConditionAnalysis} supplied to
   * {@link Joinable#makeJoinMatcher}.
   *
   * After calling this method, {@link #hasMatch()} will return whether at least one row matched. After reading that
   * row, {@link #nextMatch()} can be used to move on to the next row.
   */
  void matchCondition();

  /**
   * Matches every row that has not already been matched. Used for right joins.
   *
   * After calling this method, {@link #hasMatch()} will return whether at least one row matched. After reading that
   * row, {@link #nextMatch()} can be used to move on to the next row.
   *
   * Will only work correctly if {@link Joinable#makeJoinMatcher} was called with {@code remainderNeeded == true}.
   */
  void matchRemainder();

  /**
   * Returns whether the active matcher ({@link #matchCondition()} or {@link #matchRemainder()}) has matched something.
   */
  boolean hasMatch();

  /**
   * Moves on to the next match. It is only valid to call this if {@link #hasMatch()} is true.
   */
  void nextMatch();

  /**
   * Returns whether this matcher is currently matching the remainder (i.e. if {@link #matchRemainder()} was the
   * most recent match method called).
   */
  boolean matchingRemainder();

  /**
   * Clears any active matches. Does not clear memory about what has been matched in the past.
   */
  void reset();
}
