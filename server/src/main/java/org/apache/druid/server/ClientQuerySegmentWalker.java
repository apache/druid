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

package org.apache.druid.server;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.client.CachingClusteredClient;
import org.apache.druid.client.cache.Cache;
import org.apache.druid.client.cache.CacheConfig;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.FluentQueryRunnerBuilder;
import org.apache.druid.query.PostProcessingOperator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.ResultLevelCachingQueryRunner;
import org.apache.druid.query.RetryQueryRunner;
import org.apache.druid.query.RetryQueryRunnerConfig;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.planning.DataSourceAnalysis;
import org.apache.druid.server.initialization.ServerConfig;
import org.joda.time.Interval;

/**
 * Query handler for Broker processes (see CliBroker).
 */
public class ClientQuerySegmentWalker implements QuerySegmentWalker
{
  private final ServiceEmitter emitter;
  private final QuerySegmentWalker clusterClient;
  private final QuerySegmentWalker localClient;
  private final QueryToolChestWarehouse warehouse;
  private final RetryQueryRunnerConfig retryConfig;
  private final ObjectMapper objectMapper;
  private final ServerConfig serverConfig;
  private final Cache cache;
  private final CacheConfig cacheConfig;

  public ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      QuerySegmentWalker clusterClient,
      QuerySegmentWalker localClient,
      QueryToolChestWarehouse warehouse,
      RetryQueryRunnerConfig retryConfig,
      ObjectMapper objectMapper,
      ServerConfig serverConfig,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this.emitter = emitter;
    this.clusterClient = clusterClient;
    this.localClient = localClient;
    this.warehouse = warehouse;
    this.retryConfig = retryConfig;
    this.objectMapper = objectMapper;
    this.serverConfig = serverConfig;
    this.cache = cache;
    this.cacheConfig = cacheConfig;
  }

  @Inject
  ClientQuerySegmentWalker(
      ServiceEmitter emitter,
      CachingClusteredClient clusterClient,
      LocalQuerySegmentWalker localClient,
      QueryToolChestWarehouse warehouse,
      RetryQueryRunnerConfig retryConfig,
      ObjectMapper objectMapper,
      ServerConfig serverConfig,
      Cache cache,
      CacheConfig cacheConfig
  )
  {
    this(
        emitter,
        (QuerySegmentWalker) clusterClient,
        (QuerySegmentWalker) localClient,
        warehouse,
        retryConfig,
        objectMapper,
        serverConfig,
        cache,
        cacheConfig
    );
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

    if (analysis.isConcreteTableBased()) {
      return decorateClusterRunner(query, clusterClient.getQueryRunnerForIntervals(query, intervals));
    } else if (analysis.isConcreteBased() && analysis.isGlobal()) {
      // Concrete, non-table based, can run locally. No need to decorate since LocalQuerySegmentWalker does its own.
      return localClient.getQueryRunnerForIntervals(query, intervals);
    } else {
      // In the future, we will check here to see if parts of the query are inlinable, and if that inlining would
      // be able to create a concrete table-based query that we can run through the distributed query stack.
      throw new ISE("Query dataSource is not table-based, cannot run");
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final DataSourceAnalysis analysis = DataSourceAnalysis.forDataSource(query.getDataSource());

    if (analysis.isConcreteTableBased()) {
      return decorateClusterRunner(query, clusterClient.getQueryRunnerForSegments(query, specs));
    } else {
      throw new ISE("Query dataSource is not table-based, cannot run");
    }
  }

  private <T> QueryRunner<T> decorateClusterRunner(Query<T> query, QueryRunner<T> baseClusterRunner)
  {
    final QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);

    final PostProcessingOperator<T> postProcessing = objectMapper.convertValue(
        query.<String>getContextValue("postProcessing"),
        new TypeReference<PostProcessingOperator<T>>() {}
    );

    final QueryRunner<T> mostlyDecoratedRunner =
        new FluentQueryRunnerBuilder<>(toolChest)
            .create(
                new SetAndVerifyContextQueryRunner<>(
                    serverConfig,
                    new RetryQueryRunner<>(
                        baseClusterRunner,
                        retryConfig,
                        objectMapper
                    )
                )
            )
            .applyPreMergeDecoration()
            .mergeResults()
            .applyPostMergeDecoration()
            .emitCPUTimeMetric(emitter)
            .postProcess(postProcessing);

    // This does not adhere to the fluent workflow. See https://github.com/apache/druid/issues/5517
    return new ResultLevelCachingQueryRunner<>(
        mostlyDecoratedRunner,
        toolChest,
        query,
        objectMapper,
        cache,
        cacheConfig
    );
  }
}
