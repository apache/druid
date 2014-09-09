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

package io.druid.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.inject.Inject;
import com.metamx.emitter.service.ServiceEmitter;
import com.metamx.emitter.service.ServiceMetricEvent;
import io.druid.client.CachingClusteredClient;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.MetricsEmittingQueryRunner;
import io.druid.query.PostProcessingOperator;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.SegmentDescriptor;
import io.druid.query.UnionQueryRunner;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Map;

/**
 */
public class ClientQuerySegmentWalker implements QuerySegmentWalker
{
  private final ServiceEmitter emitter;
  private final CachingClusteredClient baseClient;
  private final QueryToolChestWarehouse warehouse;
  private final ObjectMapper objectMapper;

  @Inject
  public ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      CachingClusteredClient baseClient,
      QueryToolChestWarehouse warehouse,
      ObjectMapper objectMapper
  )
  {
    this.emitter = emitter;
    this.baseClient = baseClient;
    this.warehouse = warehouse;
    this.objectMapper = objectMapper;
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

  private <T> QueryRunner<T> makeRunner(final Query<T> query)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    final FinalizeResultsQueryRunner<T> baseRunner = new FinalizeResultsQueryRunner<T>(
        toolChest.postMergeQueryDecoration(
            toolChest.mergeResults(
                new UnionQueryRunner<T>(
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
                    ).withWaitMeasuredFromNow(),
                    toolChest
                )
            )
        ),
        toolChest
    );


    final Map<String, Object> context = query.getContext();
    PostProcessingOperator<T> postProcessing = null;
    if(context != null) {
      postProcessing = objectMapper.convertValue(
          context.get("postProcessing"),
          new TypeReference<PostProcessingOperator<T>>()
          {
          }
      );
    }

    return postProcessing != null ?
           postProcessing.postProcess(baseRunner) : baseRunner;
  }
}
