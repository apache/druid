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

package org.apache.druid.queryng.fragment;

import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;

/**
 * Provides fragment-level context to operators within a single
 * fragment.
 * <p>
 * Passed around while building a DAG of operators:
 * provides access to the {@link FragmentContext} which each operator
 * may use, and a method to register operators with the fragment.
 */
public interface FragmentContext
{
  @SuppressWarnings("unused") // Needed in the future
  long NO_TIMEOUT = -1;

  enum State
  {
    START, RUN, FAILED, CLOSED
  }

  /**
   * State of the fragment. Generally of interest to the top-level fragment
   * runner. A fragment enters the run state when it starts running, the
   * failed state when the top-level runner calls {@link #failed(), and
   * the closed state after a successful run.
   */
  State state();

  /**
   * The native query ID associated with the native query that gave rise
   * to the fragment's operator DAG. There is one query ID per fragment.
   * Nested queries provide a separate slice (which executes as a fragment)
   * for each of the subqueries.
   */
  String queryId();

  /**
   * Return the response context shared by all operators within this fragment.
   */
  @SuppressWarnings("unused") // Stop IntelliJ from complaining
  ResponseContext responseContext();

  /**
   * Register an operator for this fragment. The operator will be
   * closed automatically upon fragment completion both for the success
   * and error cases. An operator <i>may</i> be closed earlier, if a
   * DAG branch detects it is done during a run. Thus, every operator
   * must handle a call to {@code close()} when the operator is already
   * closed.
   *
   * Operators may be registered during a run, which is useful in the
   * conversion from query runners as sometimes the query runner decides
   * late what child to create.
   */
  void register(Operator<?> op);

  /**
   * Register a generic parent-child relationship. Used for single children,
   * or when child order is unimportant.
   */
  void registerChild(Operator<?> parent, Operator<?> child);

  /**
   * Register a parent-child relationship for a specific child position, as
   * in joins where the 0 (left) position has a distinct meaning from the
   * 1 (right) position.
   */
  @SuppressWarnings("unused") // Needed in future work
  void registerChild(Operator<?> parent, int posn, Operator<?> child);

  /**
   * Register a parent-child relationship between an operator and a slice
   * of this query.
   */
  void registerChild(Operator<?> parent, int posn, int sliceID);

  /**
   * Checks if a query timeout has occurred. If so, will throw
   * an unchecked exception. The operator need not catch this
   * exception: the fragment runner will unwind the stack and
   * call each operator's {@code close()} method on timeout.
   */
  void checkTimeout();

  /**
   * Record a missing segment into the response context for this fragment.
   */
  void missingSegment(SegmentDescriptor descriptor);

  /**
   * Mark the fragment as failed. Generally set at the top of the operator
   * DAG in response to an exception. Operators just throw an exception to
   * indicate failure.
   */
  boolean failed();

  /**
   * Provide an update of the profile (statistics) for a specific
   * operator. Can be done while the fragment runs, or at close time.
   * The last update wins, so metrics must be cummulative.
   */
  void updateProfile(Operator<?> op, OperatorProfile profile);
}
