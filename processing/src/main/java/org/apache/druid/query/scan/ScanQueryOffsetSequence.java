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

package org.apache.druid.query.scan;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.DelegatingYieldingAccumulator;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.java.util.common.guava.YieldingSequenceBase;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A Sequence that wraps the results of a ScanQuery and skips a given number of rows. It is used to implement
 * the "offset" feature.
 */
public class ScanQueryOffsetSequence extends YieldingSequenceBase<ScanResultValue>
{
  private final Sequence<ScanResultValue> baseSequence;
  private final long skip;

  public ScanQueryOffsetSequence(Sequence<ScanResultValue> baseSequence, long skip)
  {
    this.baseSequence = baseSequence;
    this.skip = skip;

    if (skip < 1) {
      throw new IAE("'skip' must be greater than zero");
    }
  }

  @Override
  public <OutType> Yielder<OutType> toYielder(
      final OutType initValue,
      final YieldingAccumulator<OutType, ScanResultValue> accumulator
  )
  {
    final SkippingYieldingAccumulator<OutType> skippingAccumulator = new SkippingYieldingAccumulator<>(accumulator);
    return wrapYielder(baseSequence.toYielder(initValue, skippingAccumulator), skippingAccumulator);
  }

  private <OutType> Yielder<OutType> wrapYielder(
      final Yielder<OutType> yielder,
      final SkippingYieldingAccumulator<OutType> accumulator
  )
  {
    return new Yielder<OutType>()
    {
      @Override
      public OutType get()
      {
        return yielder.get();
      }

      @Override
      public Yielder<OutType> next(OutType initValue)
      {
        return wrapYielder(yielder.next(initValue), accumulator);
      }

      @Override
      public boolean isDone()
      {
        return yielder.isDone();
      }

      @Override
      public void close() throws IOException
      {
        yielder.close();
      }
    };
  }

  private class SkippingYieldingAccumulator<OutType> extends DelegatingYieldingAccumulator<OutType, ScanResultValue>
  {
    private long skipped = 0;

    public SkippingYieldingAccumulator(final YieldingAccumulator<OutType, ScanResultValue> accumulator)
    {
      super(accumulator);
    }

    @Override
    public OutType accumulate(OutType accumulated, ScanResultValue result)
    {
      if (skipped < skip) {
        final long toSkip = skip - skipped;
        final List<?> rows = (List) result.getEvents();
        if (toSkip >= rows.size()) {
          // Skip everything.
          skipped += rows.size();
          return accumulated;
        } else {
          // Skip partially.
          final List<?> newEvents = rows.stream().skip(toSkip).collect(Collectors.toList());
          skipped += toSkip;
          return super.accumulate(
              accumulated,
              new ScanResultValue(result.getSegmentId(), result.getColumns(), newEvents)
          );
        }
      } else {
        return super.accumulate(accumulated, result);
      }
    }
  }
}
