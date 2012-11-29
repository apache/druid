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
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.metamx.common.guava.Comparators;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.client.DruidDataSource;
import com.metamx.druid.client.DruidServer;
import com.metamx.druid.master.rules.Rule;
import com.metamx.druid.master.rules.RuleMap;
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
  private final RuleMap ruleMap;
  private final Map<String, DruidServer> availableServerMap;
  private final Map<String, MinMaxPriorityQueue<ServerHolder>> historicalServers;
  private final Map<String, Rule> segmentRules;
  private final Map<String, Map<String, Integer>> segmentsInCluster;
  private final Set<DruidDataSource> dataSources;
  private final Set<DataSegment> availableSegments;
  private final Map<String, LoadQueuePeon> loadManagementPeons;
  private final ServiceEmitter emitter;
  private final long millisToWaitBeforeDeleting;
  private final List<String> messages;
  private final Map<String, Integer> assignedCount;
  private final Map<String, Integer> droppedCount;
  private final int deletedCount;
  private final int unassignedCount;
  private final long unassignedSize;
  private final Map<String, Integer> movedCount;
  private final long mergeBytesLimit;
  private final int mergeSegmentsLimit;
  private final int mergedSegmentCount;

  public DruidMasterRuntimeParams(
      long startTime,
      RuleMap ruleMap,
      Map<String, DruidServer> availableServerMap,
      Map<String, MinMaxPriorityQueue<ServerHolder>> historicalServers,
      Map<String, Rule> segmentRules,
      Map<String, Map<String, Integer>> segmentsInCluster,
      Set<DruidDataSource> dataSources,
      Set<DataSegment> availableSegments,
      Map<String, LoadQueuePeon> loadManagementPeons,
      ServiceEmitter emitter,
      long millisToWaitBeforeDeleting,
      List<String> messages,
      Map<String, Integer> assignedCount,
      Map<String, Integer> droppedCount,
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
    this.ruleMap = ruleMap;
    this.availableServerMap = availableServerMap;
    this.historicalServers = historicalServers;
    this.segmentRules = segmentRules;
    this.segmentsInCluster = segmentsInCluster;
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

  public RuleMap getRuleMap()
  {
    return ruleMap;
  }

  public Map<String, DruidServer> getAvailableServerMap()
  {
    return availableServerMap;
  }

  public Map<String, MinMaxPriorityQueue<ServerHolder>> getHistoricalServers()
  {
    return historicalServers;
  }

  public Map<String, Rule> getSegmentRules()
  {
    return segmentRules;
  }

  public Map<String, Map<String, Integer>> getSegmentsInCluster()
  {
    return segmentsInCluster;
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

  public Map<String, Integer> getAssignedCount()
  {
    return assignedCount;
  }

  public Map<String, Integer> getDroppedCount()
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
        ruleMap,
        availableServerMap,
        historicalServers,
        segmentRules,
        segmentsInCluster,
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
    private RuleMap ruleMap;
    private final Map<String, DruidServer> availableServerMap;
    private final Map<String, MinMaxPriorityQueue<ServerHolder>> historicalServers;
    private final Map<String, Rule> segmentRules;
    private final Map<String, Map<String, Integer>> segmentsInCluster;
    private final Set<DruidDataSource> dataSources;
    private final Set<DataSegment> availableSegments;
    private final Map<String, LoadQueuePeon> loadManagementPeons;
    private final List<String> messages;
    private long millisToWaitBeforeDeleting;
    private ServiceEmitter emitter;
    private Map<String, Integer> assignedCount;
    private Map<String, Integer> droppedCount;
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
      this.ruleMap = null;
      this.availableServerMap = Maps.newHashMap();
      this.historicalServers = Maps.newHashMap();
      this.segmentRules = Maps.newHashMap();
      this.segmentsInCluster = Maps.newHashMap();
      this.dataSources = Sets.newHashSet();
      this.availableSegments = Sets.newTreeSet(Comparators.inverse(DataSegment.bucketMonthComparator()));
      this.loadManagementPeons = Maps.newHashMap();
      this.messages = Lists.newArrayList();
      this.emitter = null;
      this.millisToWaitBeforeDeleting = 0;
      this.assignedCount = Maps.newHashMap();
      this.droppedCount = Maps.newHashMap();
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
        RuleMap ruleMap,
        Map<String, DruidServer> availableServerMap,
        Map<String, MinMaxPriorityQueue<ServerHolder>> historicalServers,
        Map<String, Rule> segmentRules,
        Map<String, Map<String, Integer>> segmentsInCluster,
        Set<DruidDataSource> dataSources,
        Set<DataSegment> availableSegments,
        Map<String, LoadQueuePeon> loadManagementPeons,
        List<String> messages,
        ServiceEmitter emitter,
        long millisToWaitBeforeDeleting,
        Map<String, Integer> assignedCount,
        Map<String, Integer> droppedCount,
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
      this.ruleMap = ruleMap;
      this.availableServerMap = availableServerMap;
      this.historicalServers = historicalServers;
      this.segmentRules = segmentRules;
      this.segmentsInCluster = segmentsInCluster;
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
          ruleMap,
          availableServerMap,
          historicalServers,
          segmentRules,
          segmentsInCluster,
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

    public Builder withRuleMap(RuleMap ruleMap)
    {
      this.ruleMap = ruleMap;
      return this;
    }

    public Builder withAvailableServerMap(Map<String, DruidServer> availableServersCollection)
    {
      availableServerMap.putAll(Collections.unmodifiableMap(availableServersCollection));
      return this;
    }

    public Builder withHistoricalServers(Map<String, MinMaxPriorityQueue<ServerHolder>> historicalServersMap)
    {
      historicalServers.putAll(historicalServersMap);
      return this;
    }

    public Builder withSegmentRules(Map<String, Rule> segmentRules)
    {
      this.segmentRules.putAll(segmentRules);
      return this;
    }

    public Builder withSegmentsInCluster(Map<String, Map<String, Integer>> segmentsInCluster)
    {
      this.segmentsInCluster.putAll(segmentsInCluster);
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

    public Builder withAssignedCount(Map<String, Integer> assignedCount)
    {
      this.assignedCount.putAll(assignedCount);
      return this;
    }

    public Builder withDroppedCount(Map<String, Integer> droppedCount)
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
