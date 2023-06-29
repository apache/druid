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

import javax.annotation.Nullable;
import java.io.Closeable;

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
   * Convenience method to run an Operator until completion.  Data will be pushed into the Receiver.  This is the
   * primary entry point that users of Operators will call to do their work.
   *
   * @param op       the operator to run to completion
   * @param receiver a receiver that will receive data
   */
  static void go(Operator op, Receiver receiver)
  {
    Closeable continuation = null;
    do {
      continuation = op.goOrContinue(continuation, receiver);
    } while (continuation != null);
  }

  /**
   * This is the primary workhorse method of an Operator.  That said, users of Operators are not expected to use this
   * method and instead are expected to call the static method {@link Operator#go(Operator, Receiver)}.
   * <p>
   * Data will be pushed into the Receiver.  The Receiver has the option of returning any of the {@link Signal} signals
   * to indicate its degree of readiness for more data to be received.
   * <p>
   * If a Receiver returns a {@link Signal#PAUSE} signal, then if there is processing left to do, then it is expected
   * that a non-null "continuation" object nwill be returned.  This allows for flow control to be returned to the
   * caller to, e.g., process another Operator or just exert backpressure.  In this case, when the controller wants to
   * resume, it must call this method again and include the continuation object that it received.
   * <p>
   * The continuation object is Closeable because it is possible that while processing is paused on one Operator, the
   * processing of another Operator could obviate the need for further processing.  In this case, instead of resuming
   * the paused Operation and returning {@link Signal#STOP} on the next push into the Receiver, the code must
   * call {@link Closeable#close()} on the continuation object to cancel all further processing and clean up all
   * related resources.  If, instead, the continuation object is passed back into a call to goOrContinue, then
   * close() must <em>NOT</em> be called on the continuation object.  Said again, the controller must either
   * 1) pass the continuation object back into a call to goOrContinue, OR
   * 2) call close() on the continuation object
   * and <em>NEVER</em> do both.
   * <p>
   * Once a reference to a continuation object has been passed back to a goOrContinue method, it should never be
   * reused by the controller.  This is to give Operator implementations the ability to decide whether it makes sense
   * to reuse the objects on subsequent calls or create new ones.
   * <p>
   * A null return value from this method indicates that processing is complete.  The Receiver should have had its
   * {@link Receiver#completed()} method called and any resources associated with processing have already been cleaned
   * up.  Additionally, if an exception escapes a call to this method, any resources associated with processing should have
   * been cleaned up.
   * <p>
   * For implementators of the interface, if an Operator does not have any resources of its own to clean up, then it is
   * safe to just pass through the continuation object to the caller.  However, if there are resources associated with
   * the processing that must be cleaned up, the Operator implementation must wrap the received Closeable in a new
   * Closeable that will close those resources.  In this case, when the object comes back to the Operator on a call to
   * goOrContinue, the Operator must unwrap the internal Closeable and pass that back down.  In a similar fashion,
   * if there is any state that an Operator requires to be able to resume its processing, then it is expected that the
   * Operator will cast the object back to an instance of the type that it had originally returned.
   *
   * @param receiver a receiver that will receiver data
   * @return null if processing is complete, non-null if the Receiver returned a {@link Signal#PAUSE} signal
   */
  @Nullable
  Closeable goOrContinue(Closeable continuationObject, Receiver receiver);

  /**
   * This is the return object from a receiver.  It is used to communicate to whatever is pushing the data into the
   * receiver the state of processing.  This exists because Operators can sometimes decide that no more results will
   * be needed (e.g. if the result set is being limited), in which case, they need some way to communicate this
   * to downstream processing to effectively "cancel" further computation.
   * <p>
   * It's named weird because... well, the author had a hard time coming up with a meaningful name.  Suggestions
   * for alternate names are welcome.
   */
  enum Signal
  {
    /**
     * Indicates that the downstream processing need not do anything else.  Operators that return this should avoid
     * pre-emptively calling {@link Receiver#completed()} before returning STOP.  They should instead return STOP
     * and trust that the downstream code will call {@link Receiver#completed()}.  This is because downstream code
     * *might* be pipelining computations to prepare the next set of data and if the Operator first calls
     * {@link Receiver#completed()} before communicating that no further results are needed, it delays the canceling
     * of the pipelined operations and effectively wastes CPU cycles.
     */
    STOP,
    /**
     * Inidcates that the downstream processing should pause its pushing of results and instead return a
     * continuation object that encapsulates whatever state is required to resume processing.  When this signal is
     * received, Operators that are generating data might choose to exert backpressure or otherwise pause their
     * processing efforts until called again with the returned continuation object.
     * <p>
     * If an Operator has completed its processing already when this signal is received, instead of returning a
     * continuation object, it should call {@link Receiver#completed()} and return null.
     */
    PAUSE,
    /**
     * Indicates that more data is welcome.
     */
    GO
  }

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
    Signal push(RowsAndColumns rac);

    /**
     * Used to indicate that no more data will ever come.  This is only used during the happy path and is not
     * equivalent to a {@link Closeable#close()} method.  Namely, there is no guarantee that this method
     * will be called if execution halts due to an exception from push.
     * <p>
     * It is acceptable for an implementation to eagerly close resources from this method, but it is not acceptable
     * for this method to be the sole method of managing the lifecycle of resources held by the Operator.
     */
    void completed();
  }
}
