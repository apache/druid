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

import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.fragment.FragmentHandleImpl.EmptyFragmentHandle;
import org.apache.druid.queryng.fragment.FragmentHandleImpl.FragmentOperatorHandle;
import org.apache.druid.queryng.fragment.FragmentHandleImpl.FragmentSequenceHandle;
import org.apache.druid.queryng.operators.Iterators;
import org.apache.druid.queryng.operators.Operator;

import java.util.Iterator;

/**
 * Constructs a fragment by registering a set of operators. Used during the
 * transition when query runners create operators dynamically.
 *
 * Provides a "handle" to further construct the DAG for code that does not
 * have access to this class, and provides a way to run the DAG given a
 * root node (which must be one of the registered operators.)
 */
public class FragmentBuilderImpl implements FragmentBuilder
{
  private final FragmentContextImpl context;

  public FragmentBuilderImpl(
      final String queryId,
      long timeoutMs,
      final ResponseContext responseContext)
  {
    this.context = new FragmentContextImpl(queryId, timeoutMs, responseContext);
  }

  /**
   * Adds an operator to the list of operators to close. Assumes operators are
   * added bottom-up (as is required so that operators are given their inputs)
   * so that the last operator in the list is the root we want to execute.
   */
  @Override
  public void register(Operator<?> op)
  {
    context.register(op);
  }

  @Override
  public FragmentContext context()
  {
    return context;
  }

  @Override
  public <T> FragmentHandle<T> emptyHandle()
  {
    return new EmptyFragmentHandle<T>(this);
  }

  @Override
  public <T> FragmentHandle<T> handle(Operator<T> rootOp)
  {
    return new FragmentOperatorHandle<T>(this, rootOp);
  }

  @Override
  public <T> FragmentHandle<T> handle(Sequence<T> rootOp)
  {
    return new FragmentSequenceHandle<T>(this, rootOp);
  }

  @Override
  public <T> FragmentRun<T> run(Operator<T> rootOp)
  {
    return new FragmentRunImpl<T>(context, rootOp);
  }

  @Override
  public <T> Sequence<T> runAsSequence(Operator<T> rootOp)
  {
    final FragmentRun<T> run = run(rootOp);
    return new BaseSequence<T, Iterator<T>>(
        new BaseSequence.IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            return Iterators.toIterator(run.iterator());
          }

          @Override
          public void cleanup(Iterator<T> iterFromMake)
          {
            run.close();
          }
        }
    );
  }
}
