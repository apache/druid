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

import org.junit.Assert;
import org.junit.Test;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 */
public class ResourceClosingSequenceTest
{
  @Test
  public void testSanity() throws Exception
  {
    final AtomicInteger closedCounter = new AtomicInteger(0);
    Closeable closeable = new Closeable()
    {
      @Override
      public void close() throws IOException
      {
        closedCounter.incrementAndGet();
      }
    };

    final List<Integer> nums = Arrays.asList(1, 2, 3, 4, 5);

    SequenceTestHelper.testAll(Sequences.withBaggage(Sequences.simple(nums), closeable), nums);

    Assert.assertEquals(3, closedCounter.get());

    closedCounter.set(0);
    SequenceTestHelper.testClosed(closedCounter, Sequences.withBaggage(new UnsupportedSequence(), closeable));
  }
}
