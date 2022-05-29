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

package org.apache.druid.queryng.operators.general;

import com.google.common.collect.Ordering;
import org.apache.druid.query.Query;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.AbstractMergeOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.ResultIterator;

import java.util.List;

/**
 * Performs an n-way merge on n ordered child operators.
 * <p>
 * Returns elements from the priority queue in order of increasing priority, here
 * defined as in the desired output order.
 * This is due to the fact that PriorityQueue#remove() polls from the head of the queue which is, according to
 * the PriorityQueue javadoc, "the least element with respect to the specified ordering"
 *
 * @see {@link org.apache.druid.java.util.common.guava.MergeSequence}
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#nWayMergeAndLimit}
*/
public class MergeOperator<T> extends AbstractMergeOperator<T>
{
  public static <T> MergeOperator<T> forQuery(
      FragmentContext context,
      Query<T> query,
      List<Operator<T>> children)
  {
    return new MergeOperator<T>(context, query.getResultOrdering(), children);
  }

  private final List<Operator<T>> children;

  public MergeOperator(
      FragmentContext context,
      Ordering<T> ordering,
      List<Operator<T>> children)
  {
    super(context, ordering, children.size());
    this.children = children;
    for (Operator<T> child : children) {
      context.registerChild(this, child);
    }
  }

  @Override
  public ResultIterator<T> open()
  {
    for (Operator<T> child : children) {
      try {
        ResultIterator<T> childIter = child.open();
        merger.add(new OperatorInput<T>(child, childIter, childIter.next()));
      }
      catch (ResultIterator.EofException e) {
        child.close(true);
      }
    }
    return merger;
  }
}
