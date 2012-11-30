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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Comparators;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.collect.CountingMap;
import com.metamx.emitter.service.ServiceEmitter;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 */
public class DruidMasterRuntimeParams
{
  private final long startTime;
  private final DruidCluster druidCluster;
  private final SegmentRuleLookup segmentRuleLookup;
  private final SegmentReplicantLookup segmentReplicantLookup;
  private final Set<DruidDataSource> dataSources;
  private final Set<DataSegment> availableSegments;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ServiceEmitter emitter;
  private final long millisToWaitBeforeDeleting;
  private final List<String> messages;
  private final CountingMap<String> assignedCount;
  private final CountingMap<String> droppedCount;
  private final int deletedCount;
  private final int unassignedCount;
  private final long unassignedSize;
  private final Map<String, Integer> movedCount;
  private final long mergeBytesLimit;
  private final int mergeSegmentsLimit;
  private final int mergedSegmentCount;

  public DruidMasterRuntimeParams(
      long startTime,
      DruidCluster druidCluster,
      SegmentRuleLookup segmentRuleLookup,
      SegmentReplicantLookup segmentReplicantLookup,
      Set<DruidDataSource> dataSources,
      Set<DataSegment> availableSegments,
      Map<String, LoadQueuePeon> loadManagementPeons,
      ServiceEmitter emitter,
      long millisToWaitBeforeDeleting,
      List<String> messages,
      CountingMap<String> assignedCount,
      CountingMap<String> droppedCount,
      int deletedCount,
      int unassignedCount,
      long unassignedSize,
      Map<String, Integer> movedCount,
      long mergeBytesLimit,
      int mergeSegmentsLimit,
      int mergedSegmentCount
  )
  {
    this.startTime = startTime;
    this.druidCluster = druidCluster;
    this.segmentRuleLookup = segmentRuleLookup;
    this.segmentReplicantLookup = segmentReplicantLookup;
    this.dataSources = dataSources;
    this.availableSegments = availableSegments;
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
    this.mergeBytesLimit = mergeBytesLimit;
    this.mergeSegmentsLimit = mergeSegmentsLimit;
    this.mergedSegmentCount = mergedSegmentCount;
  }

  public long getStartTime()
  {
    return startTime;
  }

  public DruidCluster getDruidCluster()
  {
    return druidCluster;
  }

  public SegmentRuleLookup getSegmentRuleLookup()
  {
    return segmentRuleLookup;
  }

  public SegmentReplicantLookup getSegmentReplicantLookup()
  {
    return segmentReplicantLookup;
  }

  public Set<DruidDataSource> getDataSources()
  {
    return dataSources;
  }

  public Set<DataSegment> getAvailableSegments()
  {
    return availableSegments;
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

  public CountingMap<String> getAssignedCount()
  {
    return assignedCount;
  }

  public CountingMap<String> getDroppedCount()
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

  public long getUnassignedSize()
  {
    return unassignedSize;
  }

  public Map<String, Integer> getMovedCount()
  {
    return movedCount;
  }

  public long getMergeBytesLimit()
  {
    return mergeBytesLimit;
  }

  public int getMergeSegmentsLimit()
  {
    return mergeSegmentsLimit;
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
        druidCluster,
        segmentRuleLookup,
        segmentReplicantLookup,
        dataSources,
        availableSegments,
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
        mergeBytesLimit,
        mergeSegmentsLimit,
        mergedSegmentCount
    );
  }

  public static class Builder
  {
    private long startTime;
    private DruidCluster druidCluster;
    private SegmentRuleLookup segmentRuleLookup;
    private SegmentReplicantLookup segmentReplicantLookup;
    private final Set<DruidDataSource> dataSources;
    private final Set<DataSegment> availableSegments;
    private final Map<String, LoadQueuePeon> loadManagementPeons;
    private final List<String> messages;
    private long millisToWaitBeforeDeleting;
    private ServiceEmitter emitter;
    private CountingMap<String> assignedCount;
    private CountingMap<String> droppedCount;
    private int deletedCount;
    private int unassignedCount;
    private long unassignedSize;
    private Map<String, Integer> movedCount;
    private long mergeBytesLimit;
    private int mergeSegmentsLimit;
    private int mergedSegmentCount;

    Builder()
    {
      this.startTime = 0;
      this.druidCluster = null;
      this.segmentRuleLookup = null;
      this.segmentReplicantLookup = null;
      this.dataSources = Sets.newHashSet();
      this.availableSegments = Sets.newTreeSet(Comparators.inverse(DataSegment.bucketMonthComparator()));
      this.loadManagementPeons = Maps.newHashMap();
      this.messages = Lists.newArrayList();
      this.emitter = null;
      this.millisToWaitBeforeDeleting = 0;
      this.assignedCount = new CountingMap<String>();
      this.droppedCount = new CountingMap<String>();
      this.deletedCount = 0;
      this.unassignedCount = 0;
      this.unassignedSize = 0;
      this.movedCount = Maps.newHashMap();
      this.mergeBytesLimit = 0;
      this.mergeSegmentsLimit = 0;
      this.mergedSegmentCount = 0;
    }

    Builder(
        long startTime,
        DruidCluster cluster,
        SegmentRuleLookup segmentRuleLookup,
        SegmentReplicantLookup segmentReplicantLookup,
        Set<DruidDataSource> dataSources,
        Set<DataSegment> availableSegments,
        Map<String, LoadQueuePeon> loadManagementPeons,
        List<String> messages,
        ServiceEmitter emitter,
        long millisToWaitBeforeDeleting,
        CountingMap<String> assignedCount,
        CountingMap<String> droppedCount,
        int deletedCount,
        int unassignedCount,
        long unassignedSize,
        Map<String, Integer> movedCount,
        long mergeBytesLimit,
        int mergeSegmentsLimit,
        int mergedSegmentCount
    )
    {
      this.startTime = startTime;
      this.druidCluster = cluster;
      this.segmentRuleLookup = segmentRuleLookup;
      this.segmentReplicantLookup = segmentReplicantLookup;
      this.dataSources = dataSources;
      this.availableSegments = availableSegments;
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
      this.mergeBytesLimit = mergeBytesLimit;
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      this.mergedSegmentCount = mergedSegmentCount;
    }

    public DruidMasterRuntimeParams build()
    {
      return new DruidMasterRuntimeParams(
          startTime,
          druidCluster,
          segmentRuleLookup,
          segmentReplicantLookup,
          dataSources,
          availableSegments,
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
          mergeBytesLimit,
          mergeSegmentsLimit,
          mergedSegmentCount
      );
    }

    public Builder withStartTime(long time)
    {
      startTime = time;
      return this;
    }

    public Builder withDruidCluster(DruidCluster cluster)
    {
      this.druidCluster = cluster;
      return this;
    }

    public Builder withSegmentRuleLookup(SegmentRuleLookup segmentRuleLookup)
    {
      this.segmentRuleLookup = segmentRuleLookup;
      return this;
    }

    public Builder withSegmentReplicantLookup(SegmentReplicantLookup lookup)
    {
      this.segmentReplicantLookup = lookup;
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

    public Builder withAssignedCount(CountingMap<String> assignedCount)
    {
      this.assignedCount.putAll(assignedCount);
      return this;
    }

    public Builder withDroppedCount(CountingMap<String> droppedCount)
    {
      this.droppedCount.putAll(droppedCount);
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

    public Builder withUnassignedSize(long unassignedSize)
    {
      this.unassignedSize = unassignedSize;
      return this;
    }

    public Builder withMovedCount(Map<String, Integer> movedCount)
    {
      this.movedCount.putAll(movedCount);
      return this;
    }

    public Builder withMergeBytesLimit(long mergeBytesLimit)
    {
      this.mergeBytesLimit = mergeBytesLimit;
      return this;
    }

    public Builder withMergeSegmentsLimit(int mergeSegmentsLimit)
    {
      this.mergeSegmentsLimit = mergeSegmentsLimit;
      return this;
    }

    public Builder withMergedSegmentCount(int mergedSegmentCount)
    {
      this.mergedSegmentCount = mergedSegmentCount;
      return this;
    }
  }
}
