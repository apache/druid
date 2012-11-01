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

package com.metamx.druid.master;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import com.metamx.emitter.service.AlertEvent;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Replicator is responsible for creating and destroying replicants in the cluster.
 */
public class DruidMasterReplicator implements DruidMasterHelper
{
  private static final Logger log = new Logger(DruidMasterReplicator.class);
  private static final int MAX_NUM_SEGMENTS = 2;
  private static final int MAX_TOTAL_REPLICANTS_CREATED = 10;
  private static final int MAX_TOTAL_REPLICANTS_DESTROYED = 10;

  private final ConcurrentHashMap<String, ReplicatorSegmentHolder> currentlyCloningSegments =
      new ConcurrentHashMap<String, ReplicatorSegmentHolder>();
  private final ConcurrentHashMap<String, ReplicatorSegmentHolder> currentlyRemovingSegments =
      new ConcurrentHashMap<String, ReplicatorSegmentHolder>();

  private final Set<ReplicatorSegmentHolder> segmentsToClone = Sets.newHashSet();
  private final Set<ReplicatorSegmentHolder> segmentsToDrop = Sets.newHashSet();

  private final DruidMaster master;

  public DruidMasterReplicator(
      DruidMaster master
  )
  {
    this.master = master;
  }

  private void reduceLifetimes(
      String type,
      DruidMasterRuntimeParams params,
      Map<String, ReplicatorSegmentHolder> holders
  )
  {
    for (ReplicatorSegmentHolder holder : holders.values()) {
      holder.reduceLifetime();
      if (holder.getLifetime() <= 0) {
        params.getEmitter().emit(
            new AlertEvent.Builder().build(
                String.format("Replicator %s queue has a segment stuck", type),
                ImmutableMap.<String, Object>builder()
                            .put("segment", holder.getSegment().getIdentifier())
                            .build()
            )
        );
      }
    }
  }

  @Override
  public DruidMasterRuntimeParams run(DruidMasterRuntimeParams params)
  {
    // Make sure previous replications and deletions have finished
    if (!currentlyCloningSegments.isEmpty() || !currentlyRemovingSegments.isEmpty()) {
      if (!currentlyCloningSegments.isEmpty()) {
        reduceLifetimes("clone", params, currentlyCloningSegments);
        params = params.buildFromExisting()
                       .withMessage(
                           String.format(
                               "Still waiting on %,d replicant(s) to be created",
                               currentlyCloningSegments.size()
                           )
                       )
                       .build();
      } else {
        params = params.buildFromExisting().withMessage("Created 0 replicants").build();
      }

      if (!currentlyRemovingSegments.isEmpty()) {
        reduceLifetimes("drop", params, currentlyRemovingSegments);
        params = params.buildFromExisting()
                       .withMessage(
                           String.format(
                               "Still waiting on %,d replicant(s) to be dropped",
                               currentlyRemovingSegments.size()
                           )
                       )
                       .build();
      } else {
        params = params.buildFromExisting().withMessage("Removed 0 replicants").build();
      }

      return params;
    }

    // update a count of all segments in the cluster
    segmentsToClone.clear();
    segmentsToDrop.clear();
    updateSegmentCount(params);

    // create and destroy replicants according to their count
    cloneSegments(params);
    dropSegments(params);

    return params.buildFromExisting()
                 .withMessage(String.format("Created %,d replicants", currentlyCloningSegments.size()))
                 .withMessage(String.format("Removed %,d replicants", currentlyRemovingSegments.size()))
                 .withCreatedReplicantCount(currentlyCloningSegments.size())
                 .withDestroyedReplicantCount(currentlyRemovingSegments.size())
                 .build();
  }

  private void updateSegmentCount(DruidMasterRuntimeParams params)
  {
    Map<String, ReplicatorSegmentHolder> allSegments = Maps.newHashMap();
    Collection<DruidServer> servers = params.getHistoricalServers();

    if (servers.size() <= 1) {
      log.warn("Number of servers[%,d]: nothing to replicate", servers.size());
      return;
    }

    // Get a count for each segment in the cluster
    for (DruidServer server : servers) {
      for (DruidDataSource dataSource : server.getDataSources()) {
        for (DataSegment segment : dataSource.getSegments()) {
          String key = segment.getIdentifier();
          ReplicatorSegmentHolder holder = new ReplicatorSegmentHolder(server, segment);

          if (params.getAvailableSegments().contains(segment)) {
            if (allSegments.containsKey(key)) {
              allSegments.get(key).update(server);
            } else {
              allSegments.put(key, holder);
            }
          }
        }
      }
    }

    // Find segments to clone and drop
    for (ReplicatorSegmentHolder holder : allSegments.values()) {
      if ((holder.getCount() < MAX_NUM_SEGMENTS) &&
          (segmentsToClone.size() < MAX_TOTAL_REPLICANTS_CREATED)) {
        segmentsToClone.add(holder);
      } else if ((holder.getCount() > MAX_NUM_SEGMENTS) &&
                 (segmentsToDrop.size() < MAX_TOTAL_REPLICANTS_DESTROYED)) {
        segmentsToDrop.add(holder);
      }
    }
  }

  private void cloneSegments(final DruidMasterRuntimeParams params)
  {
    final Set<ServerHolder> serverHolders = Sets.newTreeSet(
        new Comparator<ServerHolder>()
        {
          @Override
          public int compare(ServerHolder lhs, ServerHolder rhs)
          {
            return lhs.getPercentUsed().compareTo(rhs.getPercentUsed());
          }
        }
    );
    serverHolders.addAll(
        Collections2.transform(
            params.getHistoricalServers(),
            new Function<DruidServer, ServerHolder>()
            {
              @Override
              public ServerHolder apply(DruidServer input)
              {
                return new ServerHolder(input, params.getLoadManagementPeons().get(input.getName()));
              }
            }
        )
    );

    for (final ReplicatorSegmentHolder segmentHolder : segmentsToClone) {
      String from = segmentHolder.getServers().get(0).getName();
      DataSegment segment = segmentHolder.getSegment();
      final String segmentName = segment.getIdentifier();

      try {
        for (ServerHolder serverHolder : serverHolders) {
          String to = serverHolder.getServer().getName();
          LoadQueuePeon peon = serverHolder.getPeon();

          if (!peon.getSegmentsToLoad().contains(segment) &&
              (serverHolder.getServer().getSegment(segmentName) == null) &&
              serverHolder.getAvailableSize() > segment.getSize()) {
            log.info("Replicating [%s] on [%s] to [%s]", segmentName, from, to);

            master.cloneSegment(
                from,
                to,
                segmentName,
                new LoadPeonCallback()
                {
                  @Override
                  protected void execute()
                  {
                    currentlyCloningSegments.remove(segmentName);
                  }
                }
            );
            currentlyCloningSegments.put(segmentName, segmentHolder);
            break;
          }
        }
      }
      catch (Exception e) {
        log.warn("Exception occured [%s]", e.getMessage());
        continue;
      }
    }
  }

  private void dropSegments(DruidMasterRuntimeParams params)
  {
    for (final ReplicatorSegmentHolder holder : segmentsToDrop) {
      String from = holder.getServers().get(0).getName();
      DataSegment segment = holder.getSegment();
      final String segmentName = segment.getIdentifier();

      if (!params.getLoadManagementPeons().get(from).getSegmentsToDrop().contains(segment)) {
        log.info("Dropping [%s] on [%s]", segmentName, from);
        try {
          master.dropSegment(
              from,
              segmentName,
              new LoadPeonCallback()
              {
                @Override
                protected void execute()
                {
                  currentlyRemovingSegments.remove(segmentName);
                }
              }
          );
          currentlyRemovingSegments.put(segmentName, holder);
        }
        catch (Exception e) {
          log.warn("Exception occurred [%s]", e.getMessage());
          continue;
        }
      }
    }
  }

  private class ReplicatorSegmentHolder
  {
    private final DataSegment segment;

    private final List<DruidServer> servers = Lists.newArrayList();
    private volatile int lifetime = 15;

    public ReplicatorSegmentHolder(
        DruidServer server,
        DataSegment segment
    )
    {
      this.servers.add(server);
      this.segment = segment;
    }

    public void update(DruidServer server)
    {
      servers.add(server);
    }

    public int getCount()
    {
      return servers.size();
    }

    public List<DruidServer> getServers()
    {
      return servers;
    }

    public DataSegment getSegment()
    {
      return segment;
    }

    public int getLifetime()
    {
      return lifetime;
    }

    public void reduceLifetime()
    {
      lifetime--;
    }
  }
}
