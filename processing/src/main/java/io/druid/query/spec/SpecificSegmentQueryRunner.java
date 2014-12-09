/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.spec;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.Result;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.SegmentMissingException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 */
public class SpecificSegmentQueryRunner<T> implements QueryRunner<T>
{
  private final QueryRunner<T> base;
  private final SpecificSegmentSpec specificSpec;

  public SpecificSegmentQueryRunner(
      QueryRunner<T> base,
      SpecificSegmentSpec specificSpec
  )
  {
    this.base = base;
    this.specificSpec = specificSpec;
  }

  @Override
  public Sequence<T> run(final Query<T> input, final Map<String, Object> responseContext)
  {
    final Query<T> query = input.withQuerySegmentSpec(specificSpec);

    final Thread currThread = Thread.currentThread();
    final String currThreadName = currThread.getName();
    final String newName = String.format("%s_%s_%s", query.getType(), query.getDataSource(), query.getIntervals());

    final Sequence<T> baseSequence = doNamed(
        currThread, currThreadName, newName, new Callable<Sequence<T>>()
        {
          @Override
          public Sequence<T> call() throws Exception
          {
            return base.run(query, responseContext);
          }
        }
    );

    return new Sequence<T>()
    {
      @Override
      public <OutType> OutType accumulate(final OutType initValue, final Accumulator<OutType, T> accumulator)
      {
        return doItNamed(
            new Callable<OutType>()
            {
              @Override
              public OutType call() throws Exception
              {
                try {
                  return baseSequence.accumulate(initValue, accumulator);
                }
                catch (SegmentMissingException e) {
                  List<SegmentDescriptor> missingSegments = (List<SegmentDescriptor>) responseContext.get(Result.MISSING_SEGMENTS_KEY);
                  if (missingSegments == null) {
                    missingSegments = Lists.newArrayList();
                    responseContext.put(Result.MISSING_SEGMENTS_KEY, missingSegments);
                  }
                  missingSegments.add(specificSpec.getDescriptor());
                  return initValue;
                }
              }
            }
        );
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(
          final OutType initValue,
          final YieldingAccumulator<OutType, T> accumulator
      )
      {
        return doItNamed(
            new Callable<Yielder<OutType>>()
            {
              @Override
              public Yielder<OutType> call() throws Exception
              {
                return makeYielder(baseSequence.toYielder(initValue, accumulator));
              }
            }
        );
      }

      private <OutType> Yielder<OutType> makeYielder(final Yielder<OutType> yielder)
      {
        return new Yielder<OutType>()
        {
          @Override
          public OutType get()
          {
            return yielder.get();
          }

          @Override
          public Yielder<OutType> next(final OutType initValue)
          {
            return doItNamed(
                new Callable<Yielder<OutType>>()
                {
                  @Override
                  public Yielder<OutType> call() throws Exception
                  {
                    return yielder.next(initValue);
                  }
                }
            );
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

      private <RetType> RetType doItNamed(Callable<RetType> toRun)
      {
        return doNamed(currThread, currThreadName, newName, toRun);
      }
    };
  }

  private <RetType> RetType doNamed(Thread currThread, String currName, String newName, Callable<RetType> toRun)
  {
    try {
      currThread.setName(newName);
      return toRun.call();
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
    finally {
      currThread.setName(currName);
    }
  }
}
