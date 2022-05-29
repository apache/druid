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

package org.apache.druid.queryng.operators.general;

import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;
import org.apache.druid.queryng.operators.WrappingOperator;

/**
 * Operator which throttles execution of the DAG using a mechanism
 * provided by the planner which sets up the DAG. Guarantees resources
 * are released upon DAG closure.
 * <p>
 * This operator is temporary: since it only handles contextual
 * operations, its functionality should be moved into the
 * {@link FragmentContext} at some later time.
 *
 * @see {@code TestClusterQuerySegmentWalker}
 */
public class ThrottleOperator<T> extends WrappingOperator<T>
{
  /**
   * The surrounding implementation provides an implementation which
   * wraps any information needed to do the prioritization, along with
   * any state required to lock resources, etc.
   */
  public interface Throttle
  {
    /**
     * Gather resources for execution, and perhaps block execution
     * until resources are available.
     */
    void accept();

    /**
     * Mark the end of execution. Release resources provided in
     * {@link #accept()}.
     */
    void release();
  }

  private final Throttle throttle;
  private State state = State.START;
  private long waitTimeMs;

  public ThrottleOperator(
      FragmentContext context,
      Operator<T> input,
      Throttle throttle)
  {
    super(context, input);
    this.throttle = throttle;
  }

  @Override
  protected void onOpen()
  {
    long startTimeMs = System.currentTimeMillis();
    throttle.accept();
    waitTimeMs = System.currentTimeMillis() - startTimeMs;
    state = State.RUN;
  }

  @Override
  protected void onClose()
  {
    if (state == State.RUN) {
      throttle.release();
      OperatorProfile profile = new OperatorProfile("throttle");
      profile.add("wait-time-ms", waitTimeMs);
      context.updateProfile(this, profile);
    }
    state = State.CLOSED;
  }
}
