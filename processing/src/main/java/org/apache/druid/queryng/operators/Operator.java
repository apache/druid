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
 * An operator is a data pipeline transform: something that changes a stream of
 * results in some way. An operator has a very simple lifecycle:
 * <p>
 * <ul>
 * <li>Created.</li>
 * <li>Opened (once, bottom up in the operator DAG), which provides an
 * iterator over the results to this operator.</li>
 * <li>Closed.</li>
 * </ul>
 * <p>
 * Leaf operators produce results, internal operators transform them, and root
 * operators do something with the results. Operators know nothing about their
 * children other than that they follow the operator protocol and will produce a
 * result when asked. Operators must agree on the type of the shared results.
 * <p>
 * Unlike traditional <code>QueryRunner</code>s, an operator does not create its
 * children: that is the job of the planner that created a tree of operator
 * definitions. The operator simply accepts the previously-created children and
 * does its thing.
 * <p>
 * Operators are assumed to be stateful since most data transform operations
 * involve some kind of state. The {@code start()} and {@code close()}
 * methods provide a well-defined way
 * to handle resource. Operators are created as a DAG. Unlike a simple iterator,
 * operators should <i>not</i> obtain resources in their constructors. Instead,
 * they should obtain resources, open files or otherwise start things whirring in
 * the {#code start()} method. In some cases, resource may be obtained a bit later,
 * in the first call to {@code hasNext()}. In either case, resources should be
 * released in {@code close()}.
 * <p>
 * Operators may appear in a branch of the query DAG that can be deferred or
 * closed early. Typical example: a UNION operator that starts its first child,
 * runs it, closes it, then moves to the next child. This form of operation ensures
 * resources are held for the briefest possible period of time.
 * <p>
 * To make this work, operators should cascade <code>start()</code> and
 * <code>close()</code> operations to their children. The query runner will
 * start the root operator: the root must start is children, and so on. Closing is
 * a bit more complex. Operators should close their children by cascading the
 * close operation: but only if that call came from a parent operator (as
 * indicated by the {@code cascade} parameter set to {@code true}.)
 * <p>
 * The {@link FragmentRunner} will ensure all operators are closed by calling:
 * close, from the bottom up, at the completion (successful or otherwise) of the
 * query. In this case, the  {@code cascade} parameter set to {@code false},
 * and each operator <i>should not</i> cascade this call to its children: the
 * fragment runner will do so.
 * <p>
 * Implementations can assume that calls to <code>next()</code> and
 * <code>get()</code> occur within a single thread, so state can be maintained
 * in normal (not atomic) variables.
 * <p>
 * Operators are one-pass: they are not re-entrant and cannot be restarted. We
 * assume that they read ephemeral values: once returned, they are gone. We also
 * assume the results are volatile: once read, we may not be able to read the
 * same set of values a second time even if we started the whole process over.
 * <p>
 * The type of the values returned by the operator is determined by context.
 * Druid native queries use a variety of Java objects: there is no single
 * "row" class or interface.
 * <p>
 * Operators do not have a return type parameter. Operators are generally created
 * dynamically: that code is far simpler without having to deal with unknown
 * types. Even test code will often call {@code assertEquals()} and the like
 * which don't need the type.
 * <p>
 * Having {@code open()} return the iterator for results accomplishes two goals.
 * First is the minor benefit of ensuring that an operator is opened before
 * fetching results. More substantially, this approach allows "wrapper" operators
 * which only perform work in the open or close method. For those, the open
 * method returns the iterator of the child, avoiding the overhead of pass-through
 * calls for each data batch. The wrapper operator will not sit on the data
 * path, only on the control (open/close) path.
 *
 * @param <T> the type of the object (row, batch) returned by {@link #next()}.
 */
public interface Operator<T>
{
  /**
   * Convenience interface for an operator which is its own iterator.
   */
  public interface IterableOperator<T> extends Operator<T>, Iterator<T>
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
