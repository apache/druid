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

package org.apache.druid.msq.querykit;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.Order;
import org.joda.time.Interval;

/**
 * Decides whether a base input can be skipped because none of its rows can contribute to the stage's output.
 * <p>
 * Used by {@link ReadableInputQueue} to avoid acquiring and reading such inputs.
 */
@FunctionalInterface
public interface InputPruner
{
  /**
   * Pruner that never skips an input and imposes no time order. Used by anything that does not prune.
   */
  InputPruner NEVER = (_) -> false;

  /**
   * Returns true if an input whose rows all fall within {@code interval} cannot contribute to the stage's output.
   * <p>
   * Returning false is always safe. Inputs with no known time bounds are checked with {@link Intervals#ETERNITY}.
   * Once this returns true for an interval, it must keep returning true for it: skipped inputs are dropped for good.
   */
  boolean canSkip(Interval interval);

  /**
   * Time order in which {@link ReadableInputQueue} hands out inputs. {@link Order#NONE} for no particular order.
   */
  default Order getTimeOrder()
  {
    return Order.NONE;
  }
}
