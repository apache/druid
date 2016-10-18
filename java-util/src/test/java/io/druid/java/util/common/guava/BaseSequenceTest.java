/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.java.util.common.guava;

import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class BaseSequenceTest
{
  @Test
  public void testSanity() throws Exception
  {
    final List<Integer> vals = Arrays.asList(1, 2, 3, 4, 5);
    SequenceTestHelper.testAll(BaseSequence.simple(vals), vals);
  }

  @Test
  public void testNothing() throws Exception
  {
    final List<Integer> vals = Arrays.asList();
    SequenceTestHelper.testAll(BaseSequence.simple(vals), vals);
  }

  @Test
  public void testExceptionThrownInIterator() throws Exception
  {
    final AtomicInteger closedCounter = new AtomicInteger(0);
    Sequence<Integer> seq = new BaseSequence<>(
        new BaseSequence.IteratorMaker<Integer, Iterator<Integer>>()
        {
          @Override
          public Iterator<Integer> make()
          {
            return new Iterator<Integer>()
            {
              @Override
              public boolean hasNext()
              {
                throw new UnsupportedOperationException();
              }

              @Override
              public Integer next()
              {
                throw new UnsupportedOperationException();
              }

              @Override
              public void remove()
              {
                throw new UnsupportedOperationException();
              }
            };
          }

          @Override
          public void cleanup(Iterator<Integer> iterFromMake)
          {
            closedCounter.incrementAndGet();
          }
        }
    );

    SequenceTestHelper.testClosed(closedCounter, seq);
  }

}
