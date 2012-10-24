package com.metamx.druid.query;

import com.google.common.base.Function;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Yielder;
import com.metamx.common.guava.YieldingAccumulator;
import com.metamx.druid.Query;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;

import java.io.IOException;

/**
 */
public class MetricsEmittingQueryRunner<T> implements QueryRunner<T>
{
  private final ServiceEmitter emitter;
  private final Function<Query<T>, ServiceMetricEvent.Builder> builderFn;
  private final QueryRunner<T> queryRunner;

  public MetricsEmittingQueryRunner(
      ServiceEmitter emitter,
      Function<Query<T>, ServiceMetricEvent.Builder> builderFn,
      QueryRunner<T> queryRunner
  )
  {
    this.emitter = emitter;
    this.builderFn = builderFn;
    this.queryRunner = queryRunner;
  }

  @Override
  public Sequence<T> run(final Query<T> query)
  {
    final ServiceMetricEvent.Builder builder = builderFn.apply(query);

    return new Sequence<T>()
    {
      @Override
      public <OutType> OutType accumulate(OutType outType, Accumulator<OutType, T> accumulator)
      {
        OutType retVal;

        long startTime = System.currentTimeMillis();
        try {
          retVal = queryRunner.run(query).accumulate(outType, accumulator);
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

          emitter.emit(builder.build("query/time", timeTaken));
        }

        return retVal;
      }

      @Override
      public <OutType> Yielder<OutType> toYielder(OutType initValue, YieldingAccumulator<OutType, T> accumulator)
      {
        Yielder<OutType> retVal;

        long startTime = System.currentTimeMillis();
        try {
          retVal = queryRunner.run(query).toYielder(initValue, accumulator);
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
            if (!isDone() && builder.getUser10() == null) {
              builder.setUser10("short");
            }

            long timeTaken = System.currentTimeMillis() - startTime;
            emitter.emit(builder.build("query/time", timeTaken));

            yielder.close();
          }
        };
      }
    };
  }
}
