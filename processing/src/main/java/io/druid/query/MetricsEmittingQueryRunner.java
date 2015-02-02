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

package io.druid.query;

import com.google.common.base.Function;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.io.IOException;
import java.util.Map;

/**
 */
public class MetricsEmittingQueryRunner<T> implements QueryRunner<T>
{
  private static final String DEFAULT_METRIC_NAME = "query/time";

  private final ServiceEmitter emitter;
  private final Function<Query<T>, ServiceMetricEvent.Builder> builderFn;
  private final QueryRunner<T> queryRunner;
  private final long creationTime;
  private final String metricName;

  public MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      QueryRunner<T> queryRunner
  )
  {
    this(emitter, builderFn, queryRunner, DEFAULT_METRIC_NAME);
  }

  public MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      QueryRunner<T> queryRunner,
      long creationTime,
      String metricName
  )
  {
    this.emitter = emitter;
    this.builderFn = builderFn;
    this.queryRunner = queryRunner;
    this.creationTime = creationTime;
    this.metricName = metricName;
  }

  public MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      QueryRunner<T> queryRunner,
      String metricName
  )
  {
    this(emitter, builderFn, queryRunner, -1, metricName);
  }


  public MetricsEmittingQueryRunner<T> withWaitMeasuredFromNow()
  {
    return new MetricsEmittingQueryRunner<T>(emitter, builderFn, queryRunner, System.currentTimeMillis(), metricName);
  }

  @Override
  public Sequence<T> run(final Query<T> query, final Map<String, Object> responseContext)
  {
    final ServiceMetricEvent.Builder builder = builderFn.apply(query);
    String queryId = query.getId();
    if (queryId == null) {
      queryId = "";
    }
    builder.setUser8(queryId);

    return new Sequence<T>()
    {
      @Override
      public <OutType> OutType accumulate(OutType outType, Accumulator<OutType, T> accumulator)
      {
        OutType retVal;

        long startTime = System.currentTimeMillis();
        try {
          retVal = queryRunner.run(query, responseContext).accumulate(outType, accumulator);
        }
        catch (RuntimeException e) {
          builder.setUser10("failed");
          throw e;
        }
        catch (Error e) {
          builder.setUser10("failed");
          throw e;
        }
        finally {
          long timeTaken = System.currentTimeMillis() - startTime;

          emitter.emit(builder.build(metricName, timeTaken));

          if (creationTime > 0) {
            emitter.emit(builder.build("query/wait", startTime - creationTime));
          }
        }

        return retVal;
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        Yielder<OutType> retVal;

        long startTime = System.currentTimeMillis();
        try {
          retVal = queryRunner.run(query, responseContext).toYielder(initValue, accumulator);
        }
        catch (RuntimeException e) {
          builder.setUser10("failed");
          throw e;
        }
        catch (Error e) {
          builder.setUser10("failed");
          throw e;
        }

        return makeYielder(startTime, retVal, builder);
      }

      private <OutType> Yielder<OutType> makeYielder(
          final long startTime,
          final Yielder<OutType> yielder,
          final ServiceMetricEvent.Builder builder
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
            try {
              return makeYielder(startTime, yielder.next(initValue), builder);
            }
            catch (RuntimeException e) {
              builder.setUser10("failed");
              throw e;
            }
            catch (Error e) {
              builder.setUser10("failed");
              throw e;
            }
          }

          @Override
          public boolean isDone()
          {
            return yielder.isDone();
          }

          @Override
          public void close() throws IOException
          {
            try {
              if (!isDone() && builder.getUser10() == null) {
                builder.setUser10("short");
              }

              long timeTaken = System.currentTimeMillis() - startTime;
              emitter.emit(builder.build(metricName, timeTaken));

              if (creationTime > 0) {
                emitter.emit(builder.build("query/wait", startTime - creationTime));
              }
            }
            finally {
              yielder.close();
            }
          }
        };
      }
    };
  }
}
