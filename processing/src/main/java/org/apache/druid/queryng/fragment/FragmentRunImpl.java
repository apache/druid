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
import org.apache.druid.queryng.fragment.FragmentContext.State;
import org.apache.druid.queryng.operators.Iterators;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operator.ResultIterator;

import java.util.List;

public class FragmentRunImpl<T> implements FragmentRun<T>
{
  private final FragmentContextImpl context;
  private ResultIterator<T> rootIter;

  public FragmentRunImpl(FragmentContextImpl context, Operator<T> root)
  {
    Preconditions.checkState(context.state == State.START);
    this.context = context;
    try {
      rootIter = root.open();
      context.state = State.RUN;
    }
    catch (Exception e) {
      context.failed(e);
      context.state = State.FAILED;
      throw e;
    }
  }

  @Override
  public ResultIterator<T> iterator()
  {
    Preconditions.checkState(context.state == State.RUN);
    return rootIter;
  }

  @Override
  public FragmentContext context()
  {
    return context;
  }

  @Override
  public List<T> toList()
  {
    try {
      return Iterators.toList(rootIter);
    }
    finally {
      close();
    }
  }

  @Override
  public void close()
  {
    context.close();
  }
}
