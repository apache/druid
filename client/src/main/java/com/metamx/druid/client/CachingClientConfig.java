package com.metamx.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.druid.client.cache.Cache;
import com.metamx.druid.query.QueryToolChestWarehouse;
import com.metamx.emitter.service.ServiceEmitter;

/**
 * Created with IntelliJ IDEA.
 * User: himadri
 * Date: 10/30/13
 * Time: 1:21 PM
 * To change this template use File | Settings | File Templates.
 */
public class CachingClientConfig
{
  private final QueryToolChestWarehouse warehouse;
  private final TimelineServerView serverView;
  private final Cache queryCache, resultsCache;
  private final ObjectMapper objectMapper;
  private final Lifecycle lifecycle;
  private final ServiceEmitter emitter;

  public CachingClientConfig(
      QueryToolChestWarehouse warehouse,
      TimelineServerView serverView,
      ObjectMapper objectMapper,
      Lifecycle lifecycle,
      ServiceEmitter emitter,
      Cache queryCache,
      Cache resultsCache
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
