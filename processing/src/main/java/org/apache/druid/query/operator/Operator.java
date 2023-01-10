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
 * An Operator interface that intends to have implementations that align relatively closely with the Operators that
 * other databases would also tend to be implemented using.  While a lot of Operator interfaces tend to use a
 * pull-based orientation, we use a push-based interface.  This is to give us good stacktraces.  Because of the
 * organization of the go() method, the stack traces thrown out of an Operator will be
 * 1. All of the go() calls from the top-level Operator down to the leaf Operator, this part of the stacktrace gives
 * visibility into what all of the actions that we expect to happen from the operator chain are
 * 2. All of the push() calls up until the exception happens, this part of the stack trace gives us a view of all
 * of the things that have happened to the data up until the exception was thrown.
 * <p>
 * This "hour glass" structure of the stacktrace is by design.  It is very important that implementations of this
 * interface never resort to a fluent style, inheritance or other code structuring that removes the name of the active
 * Operator from the stacktrace.  It should always be possible to find ways to avoid code duplication and still keep
 * the Operator's name on the stacktrace.
 * <p>
 * The other benefit of the go() method is that it fully encapsulates the lifecycle of the underlying resources.
 * This means that it should be possible to use try/finally blocks around calls to go() in order to ensure that
 * resources are properly closed.
 */
public interface Operator
{
  /**
   * Tells the Operation to start doing its work.  Data will be pushed into the Receiver.
   *
   * @param receiver a receiver that will receive data
   */
  void go(Receiver receiver);

  interface Receiver
  {
    /**
     * Used to push data.  Return value indicates if more data will be accepted.  If false, push should not
     * be called anymore.  If push is called after it returned false, undefined things will happen.
     *
     * @param rac {@link RowsAndColumns} of data
     * @return a boolean value indicating if more data will be accepted.  If false, push should never be called
     * anymore
     */
    boolean push(RowsAndColumns rac);

    /**
     * Used to indicate that no more data will ever come.  This is only used during the happy path and is not
     * equivalent to a {@link java.io.Closeable#close()} method.  Namely, there is no guarantee that this method
     * will be called if execution halts due to an exception from push.
     *
     * It is acceptable for an implementation to eagerly close resources from this method, but it is not acceptable
     * for this method to be the sole method of managing the lifecycle of resources held by the Operator
     */
    void completed();
  }
}
