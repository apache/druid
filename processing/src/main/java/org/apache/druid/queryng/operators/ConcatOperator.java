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

import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.IterableOperator;

import java.util.Iterator;
import java.util.List;

/**
 * Concatenate a series of inputs. Simply returns the input values,
 * does not limit or coalesce batches. Starts child operators as late as
 * possible, and closes them as early as possible.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#mergeRunners}
 */
public class ConcatOperator<T> implements IterableOperator<T>
{
  public static <T> Operator<T> concatOrNot(
      FragmentContext context,
      List<Operator<T>> children)
  {
    if (children.size() > 1) {
      return new ConcatOperator<>(context, children);
    }
    return children.get(0);
  }

  private final Iterator<Operator<T>> childIter;
  private Operator<T> current;
  private ResultIterator<T> currentIter;

  public ConcatOperator(FragmentContext context, List<Operator<T>> children)
  {
    childIter = children.iterator();
    context.register(this);
  }

  @Override
  public ResultIterator<T> open()
  {
    return this;
  }

  @Override
  public T next() throws EofException
  {
    while (true) {
      if (current != null) {
        try {
          return currentIter.next();
        }
        catch (EofException e) {
          current.close(true);
          current = null;
          currentIter = null;
        }
      }
      if (!childIter.hasNext()) {
        throw Operators.eof();
      }
      current = childIter.next();
      currentIter = current.open();
    }
  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade && current != null) {
      current.close(cascade);
    }
    current = null;
    currentIter = null;
  }
}
