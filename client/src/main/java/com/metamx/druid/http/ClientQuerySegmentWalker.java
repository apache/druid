package com.metamx.druid.http;

import com.google.common.base.Function;
import com.metamx.druid.Query;
import com.metamx.druid.client.CachingClusteredClient;
import com.metamx.druid.query.FinalizeResultsQueryRunner;
import com.metamx.druid.query.MetricsEmittingQueryRunner;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.QueryToolChestWarehouse;
import com.metamx.druid.query.segment.QuerySegmentWalker;
import com.metamx.druid.query.segment.SegmentDescriptor;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import org.joda.time.Interval;

import javax.annotation.Nullable;

/**
*/
public class ClientQuerySegmentWalker implements QuerySegmentWalker
{
  private final QueryToolChestWarehouse warehouse;
  private final ServiceEmitter emitter;
  private final CachingClusteredClient baseClient;

  public ClientQuerySegmentWalker(
      QueryToolChestWarehouse warehouse,
      ServiceEmitter emitter,
      CachingClusteredClient baseClient
  )
  {
    this.warehouse = warehouse;
    this.emitter = emitter;
    this.baseClient = baseClient;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return makeRunner(query);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return makeRunner(query);
  }

  private <T> FinalizeResultsQueryRunner<T> makeRunner(final Query<T> query)
  {
    final QueryToolChest<T,Query<T>> toolChest = warehouse.getToolChest(query);
    return new FinalizeResultsQueryRunner<T>(
        toolChest.postMergeQueryDecoration(
            toolChest.mergeResults(
                new MetricsEmittingQueryRunner<T>(
                    emitter,
                    new Function<Query<T>, ServiceMetricEvent.Builder>()
                    {
                      @Override
                      public ServiceMetricEvent.Builder apply(@Nullable Query<T> input)
                      {
                        return toolChest.makeMetricBuilder(query);
                      }
                    },
                    toolChest.preMergeQueryDecoration(baseClient)
                )
            )
        ),
        toolChest
    );
  }
}
