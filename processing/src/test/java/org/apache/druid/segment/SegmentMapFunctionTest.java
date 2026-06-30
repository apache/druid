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

package org.apache.druid.segment;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class SegmentMapFunctionTest
{
  @Test
  public void testDefaultCloseIsNoop() throws IOException
  {
    SegmentMapFunction.IDENTITY.close();
  }

  @Test
  public void testThenMapPropagatesClose() throws IOException
  {
    final AtomicInteger closeCount = new AtomicInteger(0);

    final SegmentMapFunction resourceHolding = new SegmentMapFunction()
    {
      @Override
      public Optional<Segment> apply(Optional<Segment> segment)
      {
        return segment;
      }

      @Override
      public void close()
      {
        closeCount.incrementAndGet();
      }
    };

    final SegmentMapFunction mapped = resourceHolding.thenMap(segment -> segment).thenMap(segment -> segment);
    Assert.assertEquals(0, closeCount.get());

    mapped.close();
    Assert.assertEquals(1, closeCount.get());
  }
}
