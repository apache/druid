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

package org.apache.druid.java.util.common.guava;

import org.apache.druid.collections.StableLimitingSorter;

import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

/**
 * Simultaneously sorts and limits its input.
 *
 * The sort is stable, meaning that equal elements (as determined by the comparator) will not be reordered.
 */
public class TopNSequence<T> extends BaseSequence<T, Iterator<T>>
{
  public TopNSequence(
      final Sequence<T> input,
      final Comparator<T> ordering,
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
            final StableLimitingSorter<T> sorter = new StableLimitingSorter<>(ordering, limit);

            input.accumulate(
                sorter,
                (theSorter, element) -> {
                  theSorter.add(element);
                  return theSorter;
                }
            );

            // Now return them when asked
            return sorter.drain();
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
