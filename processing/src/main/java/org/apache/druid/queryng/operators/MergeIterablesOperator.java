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

import com.google.common.collect.Ordering;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.MergeResultIterator.Input;

import java.util.Iterator;
import java.util.List;

/**
 * Performs an n-way ordered merge on materialized results presented as
 * a list of iterables.
 */
public class MergeIterablesOperator<T> implements Operator<T>
{
  private static class IteratorInput<T> implements MergeResultIterator.Input<T>
  {
    private final Iterator<T> iter;
    private T currentValue;

    public IteratorInput(Iterator<T> iter)
    {
      this.iter = iter;
      this.currentValue = iter.next();
    }

    @Override
    public boolean next()
    {
      if (!iter.hasNext()) {
        return false;
      }
      currentValue = iter.next();
      return true;
    }

    @Override
    public T get()
    {
      return currentValue;
    }

    @Override
    public void close()
    {
    }
  }

  protected final FragmentContext context;
  private final List<Iterable<T>> iterables;
  protected MergeResultIterator<T> merger;

  public MergeIterablesOperator(
      FragmentContext context,
      Ordering<? super T> ordering,
      List<Iterable<T>> iterables
  )
  {
    this.context = context;
    this.iterables = iterables;
    this.merger = new MergeResultIterator<>(ordering, iterables.size());
  }

  @Override
  public ResultIterator<T> open()
  {
    for (Iterable<T> iterable : iterables) {
      Iterator<T> iter = iterable.iterator();
      if (iter.hasNext()) {
        merger.add(new IteratorInput<T>(iter));
      }
    }
    return merger;
  }

  @Override
  public void close(boolean cascade)
  {
    if (merger == null) {
      return;
    }
    merger.close(cascade);
    OperatorProfile profile = new OperatorProfile("ordered-iterable-merge");
    profile.omitFromProfile = true;
    profile.add(OperatorProfile.ROW_COUNT_METRIC, merger.rowCount);
    profile.add("input-count", iterables.size());
    context.updateProfile(this, profile);
    merger = null;
  }
}
