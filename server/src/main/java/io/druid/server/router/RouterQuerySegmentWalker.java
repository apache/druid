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

package io.druid.server.router;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.http.client.HttpClient;
import io.druid.curator.discovery.ServerDiscoveryFactory;
import io.druid.guice.annotations.Global;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChestWarehouse;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;

/**
 */
public class RouterQuerySegmentWalker implements QuerySegmentWalker
{
  private final QueryToolChestWarehouse warehouse;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  private final BrokerSelector brokerSelector;
  private final TierConfig tierConfig;

  @Inject
  public RouterQuerySegmentWalker(
      QueryToolChestWarehouse warehouse,
      ObjectMapper objectMapper,
      @Global HttpClient httpClient,
      BrokerSelector brokerSelector,
      TierConfig tierConfig
  )
  {
    this.warehouse = warehouse;
    this.objectMapper = objectMapper;
    this.httpClient = httpClient;
    this.brokerSelector = brokerSelector;
    this.tierConfig = tierConfig;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(Query<T> query, Iterable<Interval> intervals)
  {
    return makeRunner();
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    return makeRunner();
  }

  private <T> QueryRunner<T> makeRunner()
  {
    return new TierAwareQueryRunner<T>(
        warehouse,
        objectMapper,
        httpClient,
        brokerSelector,
        tierConfig
    );
  }
}
