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

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.metamx.common.logger.Logger;
import com.metamx.druid.VersionedIntervalTimeline;
import com.metamx.druid.client.selector.ServerSelector;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryToolChestWarehouse;
import com.metamx.http.client.HttpClient;
import org.codehaus.jackson.map.ObjectMapper;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

/**
 */
public class BrokerServerView implements MutableServerView
{
  private static final Logger log = new Logger(BrokerServerView.class);

  private final Object lock = new Object();

  private final ConcurrentMap<DruidServer, DirectDruidClient> clients;
  private final Map<String, ServerSelector> selectors;
  private final Map<String, VersionedIntervalTimeline<String, ServerSelector>> timelines;
  private final ConcurrentMap<ServerCallback, Executor> serverCallbacks;
  private final ConcurrentMap<SegmentCallback, Executor> segmentCallbacks;
  private final QueryToolChestWarehouse warehose;
  private final ObjectMapper smileMapper;
  private final HttpClient httpClient;

  public BrokerServerView(
      QueryToolChestWarehouse warehose,
      ObjectMapper smileMapper,
      HttpClient httpClient
  )
  {
    this.warehose = warehose;
    this.smileMapper = smileMapper;
    this.httpClient = httpClient;

    this.clients = Maps.newConcurrentMap();
    this.selectors = Maps.newHashMap();
    this.timelines = Maps.newHashMap();
    this.serverCallbacks = Maps.newConcurrentMap();
    this.segmentCallbacks = Maps.newConcurrentMap();
  }

  @Override
  public void clear()
  {
    synchronized (lock) {
      final Iterator<DruidServer> clientsIter = clients.keySet().iterator();
      while (clientsIter.hasNext()) {
        DruidServer server = clientsIter.next();
        clientsIter.remove();
        runServerCallbacks(server);
      }

      timelines.clear();

      final Iterator<ServerSelector> selectorsIter = selectors.values().iterator();
      while (selectorsIter.hasNext()) {
        final ServerSelector selector = selectorsIter.next();
        selectorsIter.remove();
        while (!selector.isEmpty()) {
          final DruidServer pick = selector.pick();
          runSegmentCallbacks(
              new Function<SegmentCallback, CallbackAction>()
              {
                @Override
                public CallbackAction apply(@Nullable SegmentCallback input)
                {
                  return input.segmentRemoved(pick, selector.getSegment());
                }
              }
          );
          selector.removeServer(pick);
        }
      }
    }
  }

  @Override
  public void addServer(DruidServer server)
  {
    QueryRunner exists = clients.put(server, new DirectDruidClient(warehose, smileMapper, httpClient, server.getHost()));
    if (exists != null) {
      log.warn("QueryRunner for server[%s] already existed!?", server);
    }
  }

  @Override
  public void removeServer(DruidServer server)
  {
    clients.remove(server);
    for (DataSegment segment : server.getSegments().values()) {
      serverRemovedSegment(server, segment.getIdentifier());
    }
    runServerCallbacks(server);
  }

  @Override
  public void serverAddedSegment(final DruidServer server, final DataSegment segment)
  {
    String segmentId = segment.getIdentifier();
    synchronized (lock) {
      log.info("Adding segment[%s] for server[%s]", segment, server);

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

      selector.addServer(server);

      runSegmentCallbacks(
          new Function<SegmentCallback, CallbackAction>()
          {
            @Override
            public CallbackAction apply(@Nullable SegmentCallback input)
            {
              return input.segmentAdded(server, segment);
            }
          }
      );
    }
  }

  @Override
  public void serverRemovedSegment(final DruidServer server, final String segmentId)
  {
    final ServerSelector selector;

    synchronized (lock) {
      log.info("Removing segment[%s] from server[%s].", segmentId, server);

      selector = selectors.get(segmentId);
      if (selector == null) {
        log.warn("Told to remove non-existant segment[%s]", segmentId);
        return;
      }

      if (!selector.removeServer(server)) {
        log.warn(
            "Asked to disassociate non-existant association between server[%s] and segment[%s]",
            server,
            segmentId
        );
      }

      if (selector.isEmpty()) {
        DataSegment segment = selector.getSegment();
        VersionedIntervalTimeline<String, ServerSelector> timeline = timelines.get(segment.getDataSource());
        selectors.remove(segmentId);

        if (timeline.remove(
            segment.getInterval(),
            segment.getVersion(),
            segment.getShardSpec().createChunk(selector)
        ) == null) {
          log.warn(
              "Asked to remove timeline entry[interval: %s, version: %s] that doesn't exist",
              segment.getInterval(),
              segment.getVersion()
          );
        }
        runSegmentCallbacks(
            new Function<SegmentCallback, CallbackAction>()
            {
              @Override
              public CallbackAction apply(@Nullable SegmentCallback input)
              {
                return input.segmentRemoved(server, selector.getSegment());
              }
            }
        );
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
      return clients.get(server);
    }
  }

  @Override
  public void registerServerCallback(Executor exec, ServerCallback callback)
  {
    serverCallbacks.put(callback, exec);
  }

  @Override
  public void registerSegmentCallback(Executor exec, SegmentCallback callback)
  {
    segmentCallbacks.put(callback, exec);
  }

  private void runSegmentCallbacks(
      final Function<SegmentCallback, CallbackAction> fn
  )
  {
    for (final Map.Entry<SegmentCallback, Executor> entry : segmentCallbacks.entrySet()) {
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == fn.apply(entry.getKey())) {
                segmentCallbacks.remove(entry.getKey());
              }
            }
          }
      );
    }
  }

  private void runServerCallbacks(final DruidServer server)
  {
    for (final Map.Entry<ServerCallback, Executor> entry : serverCallbacks.entrySet()) {
      entry.getValue().execute(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (CallbackAction.UNREGISTER == entry.getKey().serverRemoved(server)) {
                serverCallbacks.remove(entry.getKey());
              }
            }
          }
      );
    }
  }
}
