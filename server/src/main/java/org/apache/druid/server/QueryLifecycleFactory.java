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

import com.google.inject.Inject;
import org.apache.druid.client.BrokerViewOfBrokerConfig;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.GenericQueryMetricsFactory;
import org.apache.druid.query.QueryConfigProvider;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.policy.PolicyEnforcer;
import org.apache.druid.server.broker.PerSegmentTimeoutConfig;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthorizerMapper;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@LazySingleton
public class QueryLifecycleFactory
{
  private final QueryRunnerFactoryConglomerate conglomerate;
  private final QuerySegmentWalker texasRanger;
  private final GenericQueryMetricsFactory queryMetricsFactory;
  private final ServiceEmitter emitter;
  private final RequestLogger requestLogger;
  private final AuthorizerMapper authorizerMapper;
  private final QueryConfigProvider queryConfigProvider;
  private final AuthConfig authConfig;
  private final PolicyEnforcer policyEnforcer;
  private final BrokerViewOfBrokerConfig brokerViewOfBrokerConfig;

  @Inject
  public QueryLifecycleFactory(
      final QueryRunnerFactoryConglomerate conglomerate,
      final QuerySegmentWalker texasRanger,
      final GenericQueryMetricsFactory queryMetricsFactory,
      final ServiceEmitter emitter,
      final RequestLogger requestLogger,
      final AuthConfig authConfig,
      final PolicyEnforcer policyEnforcer,
      final AuthorizerMapper authorizerMapper,
      final QueryConfigProvider queryConfigProvider,
      @Nullable final BrokerViewOfBrokerConfig brokerViewOfBrokerConfig
  )
  {
    this.conglomerate = conglomerate;
    this.texasRanger = texasRanger;
    this.queryMetricsFactory = queryMetricsFactory;
    this.emitter = emitter;
    this.requestLogger = requestLogger;
    this.authorizerMapper = authorizerMapper;
    this.queryConfigProvider = queryConfigProvider;
    this.authConfig = authConfig;
    this.policyEnforcer = policyEnforcer;
    this.brokerViewOfBrokerConfig = brokerViewOfBrokerConfig;
  }

  public QueryLifecycle factorize()
  {
    final List<QueryBlocklistRule> queryBlocklist;
    final Map<String, PerSegmentTimeoutConfig> perSegmentTimeoutConfig;
    if (brokerViewOfBrokerConfig != null && brokerViewOfBrokerConfig.getDynamicConfig() != null) {
      queryBlocklist = brokerViewOfBrokerConfig.getDynamicConfig().getQueryBlocklist();
      perSegmentTimeoutConfig = brokerViewOfBrokerConfig.getDynamicConfig().getPerSegmentTimeoutConfig();
    } else {
      queryBlocklist = Collections.emptyList();
      perSegmentTimeoutConfig = Collections.emptyMap();
    }

    return new QueryLifecycle(
        conglomerate,
        texasRanger,
        queryMetricsFactory,
        emitter,
        requestLogger,
        authorizerMapper,
        queryConfigProvider,
        authConfig,
        policyEnforcer,
        queryBlocklist,
        perSegmentTimeoutConfig,
        System.currentTimeMillis(),
        System.nanoTime()
    );
  }
}
