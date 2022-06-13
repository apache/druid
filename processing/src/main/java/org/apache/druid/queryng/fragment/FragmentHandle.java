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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.queryng.operators.Operator;

/**
 * Provides a handle to a fragment and the root operator or sequence.
 * <p>
 * Primarily for the "transition period" in which operators work alongside
 * query runners, and so the query stack is built as an alternating set
 * of sequences and operators. The sequences are mostly "operator wrappers"
 * which melt way if the DAG has the structure of: <pre>
 * operator --> sequence --> operator
 * </pre>
 * <p>
 * The operator stack speaks operators, while the "traditional" query runner
 * stack speaks sequences. This handle allows converting the DAG from one
 * form to the other, and running the stack as either a root operator or
 * root sequence.
 * <p>
 * The stack can be built upwards by adding another operator or sequence.
 * The {@link #toOperator()} and {@link #toSequence()} methods convert the
 * root, as needed, to the form needed as input to the next layer.
 * <p>
 * Again, all this complexity is needed only during the interim period in
 * which operators try to "blend in" with sequences. In the longer term,
 * when the stack is just operators, this interface will become much
 * simpler.
 */
public interface FragmentHandle<T>
{
  FragmentBuilder builder();
  FragmentContext context();

  /**
   * Reports whether the root of this DAG is an operator.
   */
  boolean rootIsOperator();

  /**
   * Returns the root operator if this handle is for an operator,
   * or if it is for a sequence which wraps an operator. Returns
   * {@code null} otherwise.
   * <p>
   * Note: the reliable way to get a root operator is to call
   * {code toOperator().rootOperator()}.
   */
  Operator<T> rootOperator();

  /**
   * Reports whether the root of this DAG is a sequence.
   */
  boolean rootIsSequence();

  /**
   * Returns the root sequence if this handle is for a sequence, else
   * {@code null}. Note: the reliable way to get a root sequence is to
   * call: {@code toSequence().rootSequence()}.
   */
  Sequence<T> rootSequence();

  /**
   * Converts this DAG to one with an operator root. Returns the same
   * handle if this is already an operator, unwraps the root sequence
   * if possible, else adds an operator on top of the unwrappable
   * sequence.
   */
  FragmentHandle<T> toOperator();

  /**
   * Converts this DAG to one with a sequence as root. Returns the same
   * handle if this is already a sequence, else wraps the root operator
   * in a sequence.
   */
  FragmentHandle<T> toSequence();

  /**
   * Extend the DAG upward by adding a new operator, which becomes the
   * new root operator, if the fragment were opened after this point.
   * Can only be applied to a fragment before it is opened.
   *
   * The operator may change the type of the elements returned by the
   * new DAG.
   */
  <U> FragmentHandle<U> compose(Operator<U> newRoot);

  /**
   * Extend the DAG upward by adding a new sequence, which becomes the
   * new root sequence, if the fragment were opened after this point.
   * Can only be applied to a fragment before it is opened.
   *
   * The sequence may change the type of the elements returned by the
   * new DAG.
   */
  <U> FragmentHandle<U> compose(Sequence<U> newRoot);

  /**
   * Run the DAG starting from the current root. Handles the
   * sequence-to-operator conversion if needed.
   */
  FragmentRun<T> run();

  /**
   * Provide the fragment results as a sequence. The sequence closes the
   * fragment at sequence closure. Handles the operator-to-sequence
   * conversion if needed.
   */
  Sequence<T> runAsSequence();
}
