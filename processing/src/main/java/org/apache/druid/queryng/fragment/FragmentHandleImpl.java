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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.queryng.fragment.FragmentContext.State;
import org.apache.druid.queryng.operators.NullOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operators;

public abstract class FragmentHandleImpl<T> implements FragmentHandle<T>
{
  protected final FragmentBuilder builder;

  public FragmentHandleImpl(FragmentBuilder builder)
  {
    this.builder = builder;
  }

  @Override
  public FragmentBuilder builder()
  {
    return builder;
  }

  @Override
  public FragmentContext context()
  {
    return builder.context();
  }

  @Override
  public boolean rootIsSequence()
  {
    return !rootIsOperator();
  }

  @Override
  public Sequence<T> rootSequence()
  {
    return null;
  }

  @Override
  public <U> FragmentHandle<U> compose(Operator<U> newRoot)
  {
    Preconditions.checkState(context().state() == State.START);
    return new FragmentOperatorHandle<U>(builder, newRoot);
  }

  @Override
  public <U> FragmentHandle<U> compose(Sequence<U> newRoot)
  {
    Preconditions.checkState(context().state() == State.START);
    return new FragmentSequenceHandle<U>(builder, newRoot);
  }

  protected static class FragmentOperatorHandle<T> extends FragmentHandleImpl<T>
  {
    private final Operator<T> root;

    public FragmentOperatorHandle(FragmentBuilder builder, Operator<T> root)
    {
      super(builder);
      this.root = root;
    }

    @Override
    public FragmentRun<T> run()
    {
      return builder.run(root);
    }

    @Override
    public boolean rootIsOperator()
    {
      return true;
    }

    @Override
    public Operator<T> rootOperator()
    {
      return root;
    }

    @Override
    public Sequence<T> runAsSequence()
    {
      return builder.runAsSequence(root);
    }

    @Override
    public FragmentHandle<T> toOperator()
    {
      return this;
    }

    @Override
    public FragmentHandle<T> toSequence()
    {
      return new FragmentSequenceHandle<T>(builder, Operators.toSequence(root));
    }
  }

  protected static class FragmentSequenceHandle<T> extends FragmentHandleImpl<T>
  {
    private final Sequence<T> root;

    public FragmentSequenceHandle(FragmentBuilder builder, Sequence<T> root)
    {
      super(builder);
      this.root = root;
    }

    @Override
    public boolean rootIsOperator()
    {
      return false;
    }

    @Override
    public Sequence<T> rootSequence()
    {
      return root;
    }

    @Override
    public Operator<T> rootOperator()
    {
      return Operators.unwrapOperator(root);
    }

    @Override
    public FragmentHandle<T> toOperator()
    {
      return new FragmentOperatorHandle<T>(builder, Operators.toOperator(builder, root));
    }

    @Override
    public FragmentHandle<T> toSequence()
    {
      return this;
    }

    @Override
    public FragmentRun<T> run()
    {
      return builder.run(
          Operators.toOperator(builder, root));
    }

    @Override
    public Sequence<T> runAsSequence()
    {
      return root;
    }
  }

  protected static class EmptyFragmentHandle<T> extends FragmentHandleImpl<T>
  {
    public EmptyFragmentHandle(FragmentBuilder builder)
    {
      super(builder);
    }

    @Override
    public FragmentRun<T> run()
    {
      return builder.run(new NullOperator<T>(context()));
    }

    @Override
    public boolean rootIsOperator()
    {
      return false;
    }

    @Override
    public boolean rootIsSequence()
    {
      return false;
    }

    @Override
    public Operator<T> rootOperator()
    {
      return null;
    }

    @Override
    public Sequence<T> runAsSequence()
    {
      return Sequences.empty();
    }

    @Override
    public FragmentHandle<T> toOperator()
    {
      return this;
    }

    @Override
    public FragmentHandle<T> toSequence()
    {
      throw new ISE("Cannot convert an empty fragment to a sequence");
    }
  }
}
