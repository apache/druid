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

package org.apache.druid.queryng.operators;

import java.util.Iterator;

/**
 * An operator is a data pipeline transform: something that operates on
 * a stream of results in some way. An operator has a very simple lifecycle:
 * <p>
 * <ul>
 * <li>Created.</li>
 * <li>Opened (once, bottom up in the operator DAG), which provides an
 * iterator over the results to this operator.</li>
 * <li>Closed.</li>
 * </ul>
 * <p>
 * Leaf operators produce results, internal operators transform them, and root
 * operators deliver results to some consumer. Operators know nothing about their
 * children other than that they follow the operator protocol and will produce a
 * result when asked. Operators must agree on the type of the shared results.
 * <p>
 * Unlike traditional <code>QueryRunner</code>s, an operator does not create its
 * children: that is the job of the planner that created a tree of operators
 * The operator simply accepts the previously-created children and does its thing.
 *
 * <h4>State</h4>
 *
 * Operators are assumed to be stateful since most data transform operations
 * involve some kind of state.
 * <p>
 * The {@code open()} and {@code close()} methods provide a well-defined way
 * to handle resources. Operators are created as a DAG. Unlike a simple iterator,
 * operators should <i>not</i> obtain resources in their constructors. Instead,
 * they should obtain resources, open files or otherwise start things whirring in
 * the {#code open()} method. In some cases, resource may be obtained a bit later,
 * in the first call to {@code hasNext()}. In either case, resources should be
 * released in {@code close()}.
 * <p>
 * Operators may appear in a branch of the query DAG that can be deferred or
 * closed early. Typical example: a UNION operator that starts its first child,
 * runs it, closes it, then moves to the next child. This form of operation ensures
 * resources are held for the briefest possible period of time.
 * <p>
 * To make this work, operators cascade <code>open()</code> and
 * <code>close()</code> operations to their children. The query runner will
 * start the root operator: the root must start is children, and so on. Closing is
 * a bit more complex. Operators should close their children by cascading the
 * close operation: but only if that call came from a parent operator (as
 * indicated by the {@code cascade} parameter set to {@code true}.) In general,
 * each operator should open is children as late as possible, and close them as
 * early as possible.
 * <p>
 * A fragment runner will ensure all operators are closed by calling
 * close, from the bottom up, at the completion (successful or otherwise) of the
 * query. In this case, the  {@code cascade} parameter set to {@code false},
 * and each operator <i>should not</i> cascade this call to its children: the
 * fragment runner will do so.
 * <p>
 * Operators are one-pass: they are not re-entrant and cannot be restarted. We
 * assume that they read ephemeral values: once returned, they are gone. We also
 * assume the results are volatile: once read, we may not be able to read the
 * same set of values a second time even if we started the whole process over.
 * <p>
 * Having {@code open()} return the iterator for results accomplishes two goals.
 * First is the minor benefit of ensuring that an operator is opened before
 * fetching results. More substantially, this approach allows "wrapper" operators
 * which only perform work in the open or close method. For those, the open
 * method returns the iterator of the child, avoiding the overhead of pass-through
 * calls for each data batch. The wrapper operator will not sit on the data
 * path, only on the control (open/close) path. See {@link WrappingOperator} for
 * and example.
 *
 * <h4>Single-Threaded</h4>
 *
 * Implementations can assume that calls to the operator methods, or to the
 * iterator <code>hasNext()</code> and <code>next()</code> methods, always
 * occur within a single thread, so state can be maintained
 * in normal (not atomic) variables. If data is to be shuffled across threads,
 * then a thread- (or network-)aware shuffle mechanism is required.
 *
 * <h4>Type</h4>
 *
 * The type of the values returned by the operator is determined by its type.
 * Druid native queries use a variety of Java objects: there is no single
 * "row" class or interface.
 * <p>
 * Operator type specifies the type of values returned by its iterator.
 * Most operators require that the input and output types are the same,
 * though some may transform data from one type to another. See
 * {@link TransformOperator} for an example.
 *
 * <h4>Errors</h4>
 *
 * Any operator may fail for any number of reasons: I/O errors, resource
 * exhaustion, invalid state (such as divide-by-zero), code bugs, etc.
 * When an operator fails, it simply throws an exception. The exception
 * unwinds the stack up to the fragment runner, which is then responsible
 * for calling close on the fragment context. That then cascades close down
 * to all operators as described above. It may be that one or more operators
 * are not in a bad state, and throw an exception on close. The fragment
 * context "absorbs" those errors and continues to close all operators.
 * <p>
 * The fragment context has no knowledge of whether an operator was called
 * early. So, if an operator is particular, and wants to be closed only once,
 * then that operator should maintain state and ignore subsequent calls to
 * close. See {@link WrappingOperator} for a simple example.
 *
 * @param <T> the type of the object (row, batch) returned by the iterator
 * returned from {@link #open()}.
 */
public interface Operator<T>
{
  /**
   * Convenience interface for an operator which is its own iterator.
   */
  interface IterableOperator<T> extends Operator<T>, Iterator<T>
  {
  }

  /**
   * State used to track the lifecycle of an operator when we
   * need to know.
   */
  enum State
  {
    START, RUN, CLOSED
  }

  /**
   * Called to prepare for processing. Allows the operator to be created early in
   * the run, but resources to be obtained as late as possible in the run. An operator
   * calls{@code open()} on its children when it is ready to read its input: either
   * in the {@code open()} call for simple operators,or later, on demand, for more
   * complex operators such as in a merge or union.
   */
  Iterator<T> open();

  /**
   * Called at two distinct times. An operator may choose to close a child
   * when it is clear that the child will no longer be needed. For example,
   * a union might close its first child when it moves onto the second.
   * <p>
   * Operators should handle at least two calls to @{code close()}: an optional
   * early close, and a definite final close when the fragment runner shuts
   * down.
   * <p>
   * Because the fragment runner will ensure a final close, operators are
   * not required to ensure {@code close()} is called on children for odd
   * paths, such as errors.
   * <p>
   * If an operator needs to know if a query failed, it can check the status
   * in the fragment context for the state of the query (i.e. failed.)
   *
   * @param cascade {@code false} if this is the final call from the fragment
   * runner, {@code true} if it is an "early close" from a parent.
   */
  void close(boolean cascade);
}
