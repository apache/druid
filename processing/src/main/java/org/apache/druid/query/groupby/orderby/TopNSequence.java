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

package org.apache.druid.query.groupby.orderby;

import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Ordering;
import org.apache.druid.java.util.common.guava.Accumulator;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;

import java.util.Collections;
import java.util.Iterator;

public class TopNSequence<T> extends BaseSequence<T, Iterator<T>>
{
  public TopNSequence(
      final Sequence<T> input,
      final Ordering<T> ordering,
      final int limit
  )
  {
    super(
        new IteratorMaker<T, Iterator<T>>()
        {
          @Override
          public Iterator<T> make()
          {
            if (limit <= 0) {
              return Collections.emptyIterator();
            }

            // Materialize the topN values
            final MinMaxPriorityQueue<T> queue = MinMaxPriorityQueue
                .orderedBy(ordering)
                .maximumSize(limit)
                .create();

            input.accumulate(
                queue,
                new Accumulator<MinMaxPriorityQueue<T>, T>()
                {
                  @Override
                  public MinMaxPriorityQueue<T> accumulate(MinMaxPriorityQueue<T> theQueue, T row)
                  {
                    theQueue.offer(row);
                    return theQueue;
                  }
                }
            );

            // Now return them when asked
            return new Iterator<T>()
            {
              @Override
              public boolean hasNext()
              {
                return !queue.isEmpty();
              }

              @Override
              public T next()
              {
                return queue.poll();
              }

              @Override
              public void remove()
              {
                throw new UnsupportedOperationException();
              }
            };
          }

          @Override
          public void cleanup(Iterator<T> rowIterator)
          {
            // Nothing to do
          }
        }
    );
  }
}
