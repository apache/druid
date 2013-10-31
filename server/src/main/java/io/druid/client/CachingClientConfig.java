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

package io.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.client.cache.Cache;
import io.druid.query.QueryToolChestWarehouse;

/**
 * Configuration for Caching Clients.
 */
public class CachingClientConfig
{
  private final QueryToolChestWarehouse warehouse;
  private final TimelineServerView serverView;
  private final Cache queryCache, resultsCache;
  private final ObjectMapper objectMapper;
  private final Lifecycle lifecycle;
  private final ServiceEmitter emitter;

  @Inject
  public CachingClientConfig(
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView,
      ObjectMapper objectMapper,
      Lifecycle lifecycle,
      ServiceEmitter emitter,
      @Named("queryCache") Cache queryCache,
      @Named("resultsCache") Cache resultsCache
  )
  {
    this.warehouse = warehouse;
    this.serverView = serverView;
    this.queryCache = queryCache;
    this.resultsCache = resultsCache;
    this.objectMapper = objectMapper;
    this.lifecycle = lifecycle;
    this.emitter = emitter;
  }

  public QueryToolChestWarehouse getWarehouse()
  {
    return warehouse;
  }

  public TimelineServerView getServerView()
  {
    return serverView;
  }

  public Cache getQueryCache()
  {
    return queryCache;
  }

  public Cache getResultsCache()
  {
    return resultsCache;
  }

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  public Lifecycle getLifecycle()
  {
    return lifecycle;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }
}
