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

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Comparators;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import com.metamx.emitter.service.ServiceEmitter;

/**
 */
public class DruidMasterRuntimeParams
{
  private final long startTime;
  private final Map<String, DruidServer> availableServerMap;
  private final Set<DruidServer> historicalServers;
  private final Set<DruidDataSource> dataSources;
  private final Set<DataSegment> availableSegments;
  private final Set<DataSegment> unservicedSegments;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ServiceEmitter emitter;
  private final long millisToWaitBeforeDeleting;
  private final List<String> messages;
  private final int assignedCount;
  private final int droppedCount;
  private final int deletedCount;
  private final int unassignedCount;
  private final int unassignedSize;
  private final int movedCount;
  private final int createdReplicantCount;
  private final int destroyedReplicantCount;
  private final long mergeThreshold;
  private final int mergedSegmentCount;

  public DruidMasterRuntimeParams(
      long startTime,
      Map<String, DruidServer> availableServerMap,
      Set<DruidServer> historicalServers,
      Set<DruidDataSource> dataSources,
      Set<DataSegment> availableSegments,
      Set<DataSegment> unservicedSegments,
      Map<String, LoadQueuePeon> loadManagementPeons,
      ServiceEmitter emitter,
      long millisToWaitBeforeDeleting,
      List<String> messages,
      int assignedCount,
      int droppedCount,
      int deletedCount,
      int unassignedCount,
      int unassignedSize,
      int movedCount,
      int createdReplicantCount,
      int destroyedReplicantCount,
      long mergeThreshold,
      int mergedSegmentCount
  )
  {
    this.startTime = startTime;
    this.availableServerMap = availableServerMap;
    this.historicalServers = historicalServers;
    this.dataSources = dataSources;
    this.availableSegments = availableSegments;
    this.unservicedSegments = unservicedSegments;
    this.loadManagementPeons = loadManagementPeons;
    this.emitter = emitter;
    this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
    this.messages = messages;
    this.assignedCount = assignedCount;
    this.droppedCount = droppedCount;
    this.deletedCount = deletedCount;
    this.unassignedCount = unassignedCount;
    this.unassignedSize = unassignedSize;
    this.movedCount = movedCount;
    this.createdReplicantCount = createdReplicantCount;
    this.destroyedReplicantCount = destroyedReplicantCount;
    this.mergeThreshold = mergeThreshold;
    this.mergedSegmentCount = mergedSegmentCount;
  }

  public long getStartTime()
  {
    return startTime;
  }

  public Map<String, DruidServer> getAvailableServerMap()
  {
    return availableServerMap;
  }

  public Set<DruidServer> getHistoricalServers()
  {
    return historicalServers;
  }

  public Set<DruidDataSource> getDataSources()
  {
    return dataSources;
  }

  public Set<DataSegment> getAvailableSegments()
  {
    return availableSegments;
  }

  public Set<DataSegment> getUnservicedSegments()
  {
    return unservicedSegments;
  }

  public Map<String, LoadQueuePeon> getLoadManagementPeons()
  {
    return loadManagementPeons;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public long getMillisToWaitBeforeDeleting()
  {
    return millisToWaitBeforeDeleting;
  }

  public List<String> getMessages()
  {
    return messages;
  }

  public int getAssignedCount()
  {
    return assignedCount;
  }

  public int getDroppedCount()
  {
    return droppedCount;
  }

  public int getDeletedCount()
  {
    return deletedCount;
  }

  public int getUnassignedCount()
  {
    return unassignedCount;
  }

  public int getUnassignedSize()
  {
    return unassignedSize;
  }

  public int getMovedCount()
  {
    return movedCount;
  }

  public int getCreatedReplicantCount()
  {
    return createdReplicantCount;
  }

  public int getDestroyedReplicantCount()
  {
    return destroyedReplicantCount;
  }

  public long getMergeThreshold()
  {
    return mergeThreshold;
  }

  public int getMergedSegmentCount()
  {
    return mergedSegmentCount;
  }

  public static Builder newBuilder()
  {
    return new Builder();
  }

  public Builder buildFromExisting()
  {
    return new Builder(
        startTime,
        availableServerMap,
        historicalServers,
        dataSources,
        availableSegments,
        unservicedSegments,
        loadManagementPeons,
        messages,
        emitter,
        millisToWaitBeforeDeleting,
        assignedCount,
        droppedCount,
        deletedCount,
        unassignedCount,
        unassignedSize,
        movedCount,
        createdReplicantCount,
        destroyedReplicantCount,
        mergeThreshold,
        mergedSegmentCount
    );
  }

  public static class Builder
  {
    private long startTime;
    private final Map<String, DruidServer> availableServerMap;
    private final Set<DruidServer> historicalServers;
    private final Set<DruidDataSource> dataSources;
    private final Set<DataSegment> availableSegments;
    private final Set<DataSegment> unservicedSegments;
    private final Map<String, LoadQueuePeon> loadManagementPeons;
    private final List<String> messages;
    private long millisToWaitBeforeDeleting;
    private ServiceEmitter emitter;
    private int assignedCount;
    private int droppedCount;
    private int deletedCount;
    private int unassignedCount;
    private int unassignedSize;
    private int movedCount;
    private int createdReplicantCount;
    private int destroyedReplicantCount;
    private long mergeThreshold;
    private int mergedSegmentCount;

    Builder()
    {
      this.startTime = 0;
      this.availableServerMap = Maps.newHashMap();
      this.historicalServers = Sets.newHashSet();
      this.dataSources = Sets.newHashSet();
      this.availableSegments = Sets.newTreeSet(Comparators.inverse(DataSegment.bucketMonthComparator()));
      this.unservicedSegments = Sets.newTreeSet(Comparators.inverse(DataSegment.bucketMonthComparator()));
      this.loadManagementPeons = Maps.newHashMap();
      this.messages = Lists.newArrayList();
      this.emitter = null;
      this.millisToWaitBeforeDeleting = 0;
      this.assignedCount = 0;
      this.droppedCount = 0;
      this.deletedCount = 0;
      this.unassignedCount = 0;
      this.unassignedSize = 0;
      this.movedCount = 0;
      this.createdReplicantCount = 0;
      this.destroyedReplicantCount = 0;
      this.mergeThreshold = 0;
      this.mergedSegmentCount = 0;
    }

    Builder(
        long startTime,
        Map<String, DruidServer> availableServerMap,
        Set<DruidServer> historicalServers,
        Set<DruidDataSource> dataSources,
        Set<DataSegment> availableSegments,
        Set<DataSegment> unservicedSegments,
        Map<String, LoadQueuePeon> loadManagementPeons,
        List<String> messages,
        ServiceEmitter emitter,
        long millisToWaitBeforeDeleting,
        int assignedCount,
        int droppedCount,
        int deletedCount,
        int unassignedCount,
        int unassignedSize,
        int movedCount,
        int createdReplicantCount,
        int destroyedReplicantCount,
        long mergeThreshold,
        int mergedSegmentCount
    )
    {
      this.startTime = startTime;
      this.availableServerMap = availableServerMap;
      this.historicalServers = historicalServers;
      this.dataSources = dataSources;
      this.availableSegments = availableSegments;
      this.unservicedSegments = unservicedSegments;
      this.loadManagementPeons = loadManagementPeons;
      this.messages = messages;
      this.emitter = emitter;
      this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
      this.assignedCount = assignedCount;
      this.droppedCount = droppedCount;
      this.deletedCount = deletedCount;
      this.unassignedCount = unassignedCount;
      this.unassignedSize = unassignedSize;
      this.movedCount = movedCount;
      this.createdReplicantCount = createdReplicantCount;
      this.destroyedReplicantCount = destroyedReplicantCount;
      this.mergeThreshold = mergeThreshold;
      this.mergedSegmentCount = mergedSegmentCount;
    }

    public DruidMasterRuntimeParams build()
    {
      return new DruidMasterRuntimeParams(
          startTime,
          availableServerMap,
          historicalServers,
          dataSources,
          availableSegments,
          unservicedSegments,
          loadManagementPeons,
          emitter,
          millisToWaitBeforeDeleting,
          messages,
          assignedCount,
          droppedCount,
          deletedCount,
          unassignedCount,
          unassignedSize,
          movedCount,
          createdReplicantCount,
          destroyedReplicantCount,
          mergeThreshold,
          mergedSegmentCount
      );
    }

    public Builder withStartTime(long time)
    {
      startTime = time;
      return this;
    }

    public Builder withAvailableServerMap(Map<String, DruidServer> availableServersCollection)
    {
      availableServerMap.putAll(Collections.unmodifiableMap(availableServersCollection));
      return this;
    }

    public Builder withHistoricalServers(Collection<DruidServer> historicalServersCollection)
    {
      historicalServers.addAll(historicalServersCollection);
      return this;
    }

    public Builder withDatasources(Collection<DruidDataSource> dataSourcesCollection)
    {
      dataSources.addAll(Collections.unmodifiableCollection(dataSourcesCollection));
      return this;
    }

    public Builder withAvailableSegments(Collection<DataSegment> availableSegmentsCollection)
    {
      availableSegments.addAll(Collections.unmodifiableCollection(availableSegmentsCollection));
      return this;
    }

    public Builder withUnservicedSegments(Collection<DataSegment> unservicedSegmentsCollection)
    {
      unservicedSegments.addAll(Collections.unmodifiableCollection(unservicedSegmentsCollection));
      return this;
    }

    public Builder withLoadManagementPeons(Map<String, LoadQueuePeon> loadManagementPeonsCollection)
    {
      loadManagementPeons.putAll(Collections.unmodifiableMap(loadManagementPeonsCollection));
      return this;
    }

    public Builder withMessage(String message)
    {
      messages.add(message);
      return this;
    }

    public Builder withMessages(List<String> messagesCollection)
    {
      messages.addAll(Collections.unmodifiableList(messagesCollection));
      return this;
    }

    public Builder withEmitter(ServiceEmitter emitter)
    {
      this.emitter = emitter;
      return this;
    }

    public Builder withMillisToWaitBeforeDeleting(long millisToWaitBeforeDeleting)
    {
      this.millisToWaitBeforeDeleting = millisToWaitBeforeDeleting;
      return this;
    }

    public Builder withAssignedCount(int assignedCount)
    {
      this.assignedCount = assignedCount;
      return this;
    }

    public Builder withDroppedCount(int droppedCount)
    {
      this.droppedCount = droppedCount;
      return this;
    }

    public Builder withDeletedCount(int deletedCount)
    {
      this.deletedCount = deletedCount;
      return this;
    }

    public Builder withUnassignedCount(int unassignedCount)
    {
      this.unassignedCount = unassignedCount;
      return this;
    }

    public Builder withUnassignedSize(int unassignedSize)
    {
      this.unassignedSize = unassignedSize;
      return this;
    }

    public Builder withMovedCount(int movedCount)
    {
      this.movedCount = movedCount;
      return this;
    }

    public Builder withCreatedReplicantCount(int createdReplicantCount)
    {
      this.createdReplicantCount = createdReplicantCount;
      return this;
    }

    public Builder withDestroyedReplicantCount(int destroyedReplicantCount)
    {
      this.destroyedReplicantCount = destroyedReplicantCount;
      return this;
    }

    public Builder withMergeThreshold(long mergeThreshold)
    {
      this.mergeThreshold = mergeThreshold;
      return this;
    }

    public Builder withMergedSegmentCount(int mergedSegmentCount)
    {
      this.mergedSegmentCount = mergedSegmentCount;
      return this;
    }
  }
}
