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
package org.apache.druid.query.lookbackquery;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.inject.Inject;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.server.log.RequestLogger;

import javax.annotation.Nullable;

/**
 * The QueryToolChest for Lookback Query
 */
public class LookbackQueryToolChest extends QueryToolChest<Result<LookbackResultValue>, LookbackQuery>
{

  private final HttpClient httpClient;
  private final ServiceEmitter emitter;
  private final ObjectMapper mapper;
  private final ObjectMapper smileMapper;
  private QueryToolChestWarehouse warehouse;
  private final QuerySegmentWalker walker;
  private final RequestLogger requestLogger;
  private final LookBackQueryMetricsFactory lookBackQueryMetricsFactory;

  /**
   * Construct a LookbackQueryToolChest for processing Lookback queries. Lookback queries are expected
   * to be processed on broker nodes and never hit historical nodes.
   *
   * @param httpClient
   * @param emitter
   * @param mapper
   * @param smileMapper
   * @param walker
   */
  @Inject
  public LookbackQueryToolChest(
      @Global HttpClient httpClient,
      ServiceEmitter emitter,
      ObjectMapper mapper,
      @Smile ObjectMapper smileMapper,
      @Nullable QuerySegmentWalker walker,
      RequestLogger requestLogger
  )
  {

    this.httpClient = httpClient;
    this.emitter = emitter;
    this.mapper = mapper;
    this.smileMapper = smileMapper;
    this.walker = walker;
    this.requestLogger = requestLogger;
    this.lookBackQueryMetricsFactory = DefaultLookBackQueryMetricsFactory.instance();
  }


  /**
   * Set the warehouse parameter. This is not passed in the constructor because some environments don't
   * have a warehouse available in Guice, causing an exception to be thrown when trying to construct the
   * class. By passing the warehouse through a separate method with @Inject(optional=true) set, the value
   * can be injected when present but the default (null) taken when not present in guice.
   *
   * @param warehouse
   */
  @Inject(optional = true)
  public void setWarehouse(QueryToolChestWarehouse warehouse)
  {
    this.warehouse = warehouse;
  }

  @Override
  public QueryRunner<Result<LookbackResultValue>> mergeResults(QueryRunner<Result<LookbackResultValue>> queryRunner)
  {
    return new LookbackQueryRunner(httpClient, emitter, mapper, smileMapper, warehouse, walker, requestLogger);
  }

  @Override
  public QueryMetrics<? super LookbackQuery> makeMetrics(LookbackQuery query)
  {
    LookBackQueryMetrics lookBackQueryMetrics = lookBackQueryMetricsFactory.makeMetrics();
    lookBackQueryMetrics.query(query);
    return lookBackQueryMetrics;
  }

  @Override
  public Function<Result<LookbackResultValue>, Result<LookbackResultValue>> makePreComputeManipulatorFn(
      LookbackQuery lookbackQuery,
      MetricManipulationFn metricManipulationFn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<Result<LookbackResultValue>> getResultTypeReference()
  {
    return null;
  }
}
