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
  long NO_TIMEOUT = -1;

  enum State
  {
    START, RUN, FAILED, CLOSED
  }

  State state();
  String queryId();
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

  void registerChild(Operator<?> parent, Operator<?> child);
  void registerChild(Operator<?> parent, int posn, Operator<?> child);
  void registerChild(Operator<?> parent, int posn, int sliceID);

  /**
   * Checks if a query timeout has occurred. If so, will throw
   * an unchecked exception. The operator need not catch this
   * exception: the fragment runner will unwind the stack and
   * call each operator's {@code close()} method on timeout.
   */
  void checkTimeout();

  void missingSegment(SegmentDescriptor descriptor);

  boolean failed();
  void updateProfile(Operator<?> op, OperatorProfile profile);
}
