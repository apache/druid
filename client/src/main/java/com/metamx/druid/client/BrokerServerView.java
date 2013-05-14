/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.logger.Logger;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.selector.QueryableDruidServer;
import com.metamx.druid.client.selector.ServerSelector;
import com.metamx.druid.partition.PartitionChunk;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryToolChestWarehouse;
import com.metamx.http.client.HttpClient;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

/**
 */
public class BrokerServerView implements TimelineServerView
{
  private static final Logger log = new Logger(BrokerServerView.class);

  private final Object lock = new Object();

  private final ConcurrentMap<String, QueryableDruidServer> clients;
  private final Map<String, ServerSelector> selectors;
  private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines;

  private final QueryToolChestWarehouse warehose;
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;
  private final ServerView baseView;

  public BrokerServerView(
      QueryToolChestWarehouse warehose,
      ObjectMapper smileMapper,
      HttpClient httpClient,
      ServerView baseView,
      ExecutorService exec
  )
  {
    this.warehose = warehose;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;
    this.baseView = baseView;

    this.clients = Maps.newConcurrentMap();
    this.selectors = Maps.newHashMap();
    this.timelines = Maps.newHashMap();

    baseView.registerSegmentCallback(
        exec,
        new ServerView.SegmentCallback()
        {
          @Override
          public ServerView.CallbackAction segmentAdded(DruidServer server, DataSegment segment)
          {
            serverAddedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }

          @Override
          public ServerView.CallbackAction segmentRemoved(final DruidServer server, DataSegment segment)
          {
            serverRemovedSegment(server, segment);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );

    baseView.registerServerCallback(
        exec,
        new ServerView.ServerCallback()
        {
          @Override
          public ServerView.CallbackAction serverRemoved(DruidServer server)
          {
            removeServer(server);
            return ServerView.CallbackAction.CONTINUE;
          }
        }
    );
  }

  public void clear()
  {
    synchronized (lock) {
      final Iterator<String> clientsIter = clients.keySet().iterator();
      while (clientsIter.hasNext()) {
        clientsIter.remove();
      }

      timelines.clear();

      final Iterator<ServerSelector> selectorsIter = selectors.values().iterator();
      while (selectorsIter.hasNext()) {
        final ServerSelector selector = selectorsIter.next();
        selectorsIter.remove();
        while (!selector.isEmpty()) {
          final QueryableDruidServer pick = selector.pick();
          selector.removeServer(pick);
        }
      }
    }
  }

  private void addServer(DruidServer server)
  {
    QueryableDruidServer exists = clients.put(
        server.getName(),
        new QueryableDruidServer(server, makeDirectClient(server))
    );
    if (exists != null) {
      log.warn("QueryRunner for server[%s] already existed!?", server);
    }
  }

  private DirectDruidClient makeDirectClient(DruidServer server)
  {
    return new DirectDruidClient(warehose, smileMapper, httpClient, server.getHost());
  }

  private void removeServer(DruidServer server)
  {
    clients.remove(server.getName());
    for (DataSegment segment : server.getSegments().values()) {
      serverRemovedSegment(server, segment);
    }
  }

  private void serverAddedSegment(final DruidServer server, final DataSegment segment)
  {
    String segmentId = segment.getIdentifier();
    synchronized (lock) {
      log.debug("Adding segment[%s] for server[%s]", segment, server);

      ServerSelector selector = selectors.get(segmentId);
      if (selector == null) {
        selector = new ServerSelector(segment);

        VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
        if (timeline == null) {
          timeline = new VersionedIntervalTimeline<String, ServerSelector>(Ordering.natural());
          timelines.put(segment.getDataSource(), timeline);
        }

        timeline.add(segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector));
        selectors.put(segmentId, selector);
      }

      if (!clients.containsKey(server.getName())) {
        addServer(server);
      }
      selector.addServer(clients.get(server.getName()));
    }
  }

  private void serverRemovedSegment(DruidServer server, DataSegment segment)
  {
    String segmentId = segment.getIdentifier();
    final ServerSelector selector;

    synchronized (lock) {
      log.debug("Removing segment[%s] from server[%s].", segmentId, server);

      selector = selectors.get(segmentId);
      if (selector == null) {
        log.warn("Told to remove non-existant segment[%s]", segmentId);
        return;
      }

      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (!selector.removeServer(queryableDruidServer)) {
        log.warn(
            "Asked to disassociate non-existant association between server[%s] and segment[%s]",
            server,
            segmentId
        );
      }

      if (selector.isEmpty()) {
        VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
        selectors.remove(segmentId);

        final PartitionChunk<ServerSelector> removedPartition = timeline.remove(
            segment.getInterval(), segment.getVersion(), segment.getShardSpec().createChunk(selector)
        );

        if (removedPartition == null) {
          log.warn(
              "Asked to remove timeline entry[interval: %s, version: %s] that doesn't exist",
              segment.getInterval(),
              segment.getVersion()
          );
        }
      }
    }
  }


  @Override
  public VersionedIntervalTimeline<String, ServerSelector> getTimeline(String dataSource)
  {
    synchronized (lock) {
      return timelines.get(dataSource);
    }
  }

  @Override
  public <T> QueryRunner<T> getQueryRunner(DruidServer server)
  {
    synchronized (lock) {
      QueryableDruidServer queryableDruidServer = clients.get(server.getName());
      if (queryableDruidServer == null) {
        log.error("WTF?! No QueryableDruidServer found for %s", server.getName());
      }
      return queryableDruidServer.getClient();
    }
  }

  @Override
  public void registerServerCallback(Executor exec, ServerCallback callback)
  {
    baseView.registerServerCallback(exec, callback);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    baseView.registerSegmentCallback(exec, callback);
  }
}
