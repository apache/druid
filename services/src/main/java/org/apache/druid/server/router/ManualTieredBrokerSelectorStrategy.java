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

package org.apache.druid.server.router;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.sql.http.SqlQuery;

import javax.annotation.Nullable;

/**
 * Implementation of {@link TieredBrokerSelectorStrategy} which uses the parameter
 * {@link QueryContexts#BROKER_SERVICE_NAME} in the Query context to select the
 * Broker Service.
 * <p>
 * If the {@link #defaultManualBrokerService} is set to a valid Broker Service Name,
 * then all queries that do not specify a valid value for
 * {@link QueryContexts#BROKER_SERVICE_NAME} would be directed to the
 * {@code #defaultManualBrokerService}. Note that the {@code defaultManualBrokerService}
 * can be different from the {@link TieredBrokerConfig#getDefaultBrokerServiceName()}.
 */
public class ManualTieredBrokerSelectorStrategy implements TieredBrokerSelectorStrategy
{
  private static final Logger log = new Logger(ManualTieredBrokerSelectorStrategy.class);

  private final String defaultManualBrokerService;

  @JsonCreator
  public ManualTieredBrokerSelectorStrategy(
      @JsonProperty("defaultManualBrokerService") @Nullable String defaultManualBrokerService
  )
  {
    this.defaultManualBrokerService = defaultManualBrokerService;
  }

  @Override
  public Optional<String> getBrokerServiceName(TieredBrokerConfig tierConfig, Query<?> query)
  {
    return getBrokerServiceName(tierConfig, query.context());
  }

  @Override
  public Optional<String> getBrokerServiceName(TieredBrokerConfig config, SqlQuery sqlQuery)
  {
    return getBrokerServiceName(config, sqlQuery.queryContext());
  }

  /**
   * Determines the Broker service name from the given query context.
   */
  private Optional<String> getBrokerServiceName(
      TieredBrokerConfig tierConfig,
      QueryContext queryContext
  )
  {
    try {
      final String contextBrokerService = queryContext.getBrokerServiceName();

      if (isValidBrokerService(contextBrokerService, tierConfig)) {
        // If the broker service in the query context is valid, use that
        return Optional.of(contextBrokerService);
      } else if (isValidBrokerService(defaultManualBrokerService, tierConfig)) {
        // If the fallbackBrokerService is valid, use that
        return Optional.of(defaultManualBrokerService);
      } else {
        log.warn(
            "Could not find Broker Service [%s] or default [%s] in TieredBrokerConfig",
            contextBrokerService,
            defaultManualBrokerService
        );
        return Optional.absent();
      }
    }
    catch (Exception e) {
      log.error(e, "Error getting Broker Service name from Query Context");
      return isValidBrokerService(defaultManualBrokerService, tierConfig)
             ? Optional.of(defaultManualBrokerService) : Optional.absent();
    }
  }

  private boolean isValidBrokerService(String brokerServiceName, TieredBrokerConfig tierConfig)
  {
    return !StringUtils.isEmpty(brokerServiceName)
           && tierConfig.getTierToBrokerMap().containsValue(brokerServiceName);
  }

  @VisibleForTesting
  String getDefaultManualBrokerService()
  {
    return defaultManualBrokerService;
  }
}
