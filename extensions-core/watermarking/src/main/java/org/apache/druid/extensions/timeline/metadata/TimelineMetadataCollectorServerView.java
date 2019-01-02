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

package org.apache.druid.extensions.timeline.metadata;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.client.BrokerSegmentWatcherConfig;
import org.apache.druid.client.BrokerServerView;
import org.apache.druid.client.DruidDataSource;
import org.apache.druid.client.DruidServer;
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.selector.TierSelectorStrategy;
import org.apache.druid.guice.annotations.Client;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.QueryWatcher;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/***
 * BrokerServerView which exposes a mechanism to enumerate datasources
 */
public class TimelineMetadataCollectorServerView extends BrokerServerView
{
  private final Object lock = new Object();

  private final FilteredServerInventoryView baseView;

  @Inject
  public TimelineMetadataCollectorServerView(
      QueryToolChestWarehouse warehouse,
      QueryWatcher queryWatcher,
      @Smile ObjectMapper smileMapper,
      @Client HttpClient httpClient,
      FilteredServerInventoryView baseView,
      TierSelectorStrategy tierSelectorStrategy,
      ServiceEmitter emitter,
      final BrokerSegmentWatcherConfig segmentWatcherConfig
  )
  {
    super(
        warehouse,
        queryWatcher,
        smileMapper,
        httpClient,
        baseView,
        tierSelectorStrategy,
        emitter,
        segmentWatcherConfig
    );
    this.baseView = baseView;
  }

  public Collection<String> getDataSources()
  {
    return StreamSupport
        .stream(baseView.getInventory().spliterator(), false)
        .map(DruidServer::getDataSources)
        .flatMap(i -> StreamSupport.stream(i.spliterator(), false))
        .map(DruidDataSource::getName)
        .collect(Collectors.toSet());
  }
}
