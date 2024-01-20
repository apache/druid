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

package org.apache.druid.server.coordination;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.inject.Inject;
import com.google.inject.Provider;
import org.apache.curator.utils.ZKPaths;
import org.apache.druid.common.utils.UUIDUtils;
import org.apache.druid.curator.ZkEnablementConfig;
import org.apache.druid.curator.announcement.Announcer;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.realtime.appenderator.SegmentSchemas;
import org.apache.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import org.apache.druid.server.initialization.ZkPathsConfig;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class BatchDataSegmentAnnouncer implements DataSegmentAnnouncer
{
  private static final Logger log = new Logger(BatchDataSegmentAnnouncer.class);

  private final BatchDataSegmentAnnouncerConfig config;

  @Nullable //Null if zk is disabled or isSkipSegmentAnnouncementOnZk = true
  private final Announcer announcer;

  private final ObjectMapper jsonMapper;
  private final String liveSegmentLocation;
  private final DruidServerMetadata server;

  private final Object lock = new Object();
  private final AtomicLong counter = new AtomicLong(0);

  private final Set<SegmentZNode> availableZNodes = new ConcurrentSkipListSet<SegmentZNode>();
  private final ConcurrentMap<DataSegment, SegmentZNode> segmentLookup = new ConcurrentHashMap<>();
  private final Function<DataSegment, DataSegment> segmentTransformer;

  private final ChangeRequestHistory<DataSegmentChangeRequest> changes = new ChangeRequestHistory<>();

  private final ConcurrentMap<String, SegmentSchemas> taskSinkSchema = new ConcurrentHashMap<>();

  @Nullable
  private final SegmentZNode dummyZnode;

  private final boolean isSkipSegmentAnnouncementOnZk;

  @Inject
  public BatchDataSegmentAnnouncer(
      DruidServerMetadata server,
      final BatchDataSegmentAnnouncerConfig config,
      ZkPathsConfig zkPaths,
      Provider<Announcer> announcerProvider,
      ObjectMapper jsonMapper,
      ZkEnablementConfig zkEnablementConfig
  )
  {
    this.config = config;
    this.jsonMapper = jsonMapper;
    this.server = server;

    this.liveSegmentLocation = ZKPaths.makePath(zkPaths.getLiveSegmentsPath(), server.getName());
    segmentTransformer = input -> {
      DataSegment rv = input;
      if (config.isSkipDimensionsAndMetrics()) {
        rv = rv.withDimensions(null).withMetrics(null);
      }
      if (config.isSkipLoadSpec()) {
        rv = rv.withLoadSpec(null);
      }
      return rv;
    };

    isSkipSegmentAnnouncementOnZk = !zkEnablementConfig.isEnabled() || config.isSkipSegmentAnnouncementOnZk();
    if (isSkipSegmentAnnouncementOnZk) {
      dummyZnode = new SegmentZNode("PLACE_HOLDER_ONLY");
      this.announcer = null;
    } else {
      dummyZnode = null;
      this.announcer = announcerProvider.get();
    }
  }

  @VisibleForTesting
  public BatchDataSegmentAnnouncer(
      DruidServerMetadata server,
      final BatchDataSegmentAnnouncerConfig config,
      ZkPathsConfig zkPaths,
      Announcer announcer,
      ObjectMapper jsonMapper
  )
  {
    this(server, config, zkPaths, () -> announcer, jsonMapper, ZkEnablementConfig.ENABLED);
  }

  @LifecycleStop
  public void stop()
  {
    changes.stop();
  }


  @Override
  public void announceSegment(DataSegment segment) throws IOException
  {
    if (segmentLookup.containsKey(segment)) {
      log.info("Skipping announcement of segment [%s]. Announcement exists already.", segment.getId());
      return;
    }

    synchronized (lock) {
      if (segmentLookup.containsKey(segment)) {
        log.info("Skipping announcement of segment [%s]. Announcement exists already.", segment.getId());
        return;
      }

      DataSegment toAnnounce = segmentTransformer.apply(segment);

      changes.addChangeRequest(new SegmentChangeRequestLoad(toAnnounce));

      if (isSkipSegmentAnnouncementOnZk) {
        segmentLookup.put(segment, dummyZnode);
        return;
      }

      int newBytesLen = jsonMapper.writeValueAsBytes(toAnnounce).length;
      if (newBytesLen > config.getMaxBytesPerNode()) {
        throw new ISE("byte size %,d exceeds %,d", newBytesLen, config.getMaxBytesPerNode());
      }

      boolean done = false;
      if (!availableZNodes.isEmpty()) {
        // update existing batch
        Iterator<SegmentZNode> iter = availableZNodes.iterator();
        while (iter.hasNext() && !done) {
          SegmentZNode availableZNode = iter.next();
          if (availableZNode.getBytes().length + newBytesLen < config.getMaxBytesPerNode()) {
            availableZNode.addSegment(toAnnounce);
            log.info(
                "Announcing segment[%s] at existing path[%s]",
                toAnnounce.getId(),
                availableZNode.getPath()
            );
            announcer.update(availableZNode.getPath(), availableZNode.getBytes());
            segmentLookup.put(toAnnounce, availableZNode);

            if (availableZNode.getCount() >= config.getSegmentsPerNode()) {
              availableZNodes.remove(availableZNode);
            }
            done = true;
          } else {
            // We could have kept the znode around for later use, however we remove it since segment announcements should
            // have similar size unless there are significant schema changes. Removing the znode reduces the number of
            // znodes that would be scanned at each announcement.
            availableZNodes.remove(availableZNode);
          }
        }
      }

      if (!done) {
        assert (availableZNodes.isEmpty());
        // create new batch

        SegmentZNode availableZNode = new SegmentZNode(makeServedSegmentPath());
        availableZNode.addSegment(toAnnounce);

        log.info("Announcing %s[%s] at new path[%s]",
                 toAnnounce.isTombstone() ? DataSegment.TOMBSTONE_LOADSPEC_TYPE : "segment",
                 toAnnounce.getId(),
                 availableZNode.getPath()
        );
        announcer.announce(availableZNode.getPath(), availableZNode.getBytes());
        segmentLookup.put(toAnnounce, availableZNode);
        availableZNodes.add(availableZNode);
      }
    }
  }

  @Override
  public void unannounceSegment(DataSegment segment)
  {
    synchronized (lock) {
      final SegmentZNode segmentZNode = segmentLookup.remove(segment);

      if (segmentZNode == null) {
        log.warn("No path to unannounce segment[%s]", segment.getId());
        return;
      }

      changes.addChangeRequest(new SegmentChangeRequestDrop(segment));

      if (isSkipSegmentAnnouncementOnZk) {
        return;
      }

      segmentZNode.removeSegment(segment);

      log.info("Unannouncing segment[%s] at path[%s]", segment.getId(), segmentZNode.getPath());
      if (segmentZNode.getCount() == 0) {
        availableZNodes.remove(segmentZNode);
        announcer.unannounce(segmentZNode.getPath());
      } else {
        announcer.update(segmentZNode.getPath(), segmentZNode.getBytes());
        availableZNodes.add(segmentZNode);
      }
    }
  }

  @Override
  public void announceSegments(Iterable<DataSegment> segments) throws IOException
  {
    SegmentZNode segmentZNode = new SegmentZNode(makeServedSegmentPath());
    Set<DataSegment> batch = new HashSet<>();
    List<DataSegmentChangeRequest> changesBatch = new ArrayList<>();

    int byteSize = 0;
    int count = 0;

    synchronized (lock) {
      for (DataSegment ds : segments) {

        if (segmentLookup.containsKey(ds)) {
          log.info("Skipping announcement of segment [%s]. Announcement exists already.", ds.getId());
          return;
        }

        DataSegment segment = segmentTransformer.apply(ds);

        changesBatch.add(new SegmentChangeRequestLoad(segment));

        if (isSkipSegmentAnnouncementOnZk) {
          segmentLookup.put(segment, dummyZnode);
          continue;
        }

        int newBytesLen = jsonMapper.writeValueAsBytes(segment).length;

        if (newBytesLen > config.getMaxBytesPerNode()) {
          throw new ISE("byte size %,d exceeds %,d", newBytesLen, config.getMaxBytesPerNode());
        }

        if (count >= config.getSegmentsPerNode() || byteSize + newBytesLen > config.getMaxBytesPerNode()) {
          segmentZNode.addSegments(batch);
          announcer.announce(segmentZNode.getPath(), segmentZNode.getBytes());

          segmentZNode = new SegmentZNode(makeServedSegmentPath());
          batch = new HashSet<>();
          count = 0;
          byteSize = 0;
        }

        log.info("Announcing segment[%s] at path[%s]", segment.getId(), segmentZNode.getPath());
        segmentLookup.put(segment, segmentZNode);
        batch.add(segment);
        count++;
        byteSize += newBytesLen;
      }
    }

    changes.addChangeRequests(changesBatch);

    if (!isSkipSegmentAnnouncementOnZk) {
      segmentZNode.addSegments(batch);
      announcer.announce(segmentZNode.getPath(), segmentZNode.getBytes());
    }
  }

  @Override
  public void unannounceSegments(Iterable<DataSegment> segments)
  {
    for (DataSegment segment : segments) {
      unannounceSegment(segment);
    }
  }

  @Override
  public void announceSegmentSchemas(
      String taskId,
      SegmentSchemas segmentSchemas,
      SegmentSchemas segmentSchemasChange
  )
  {
    log.info("Announcing sink schema for task [%s], absolute schema [%s], delta schema [%s].",
             taskId, segmentSchemas, segmentSchemasChange
    );

    taskSinkSchema.put(taskId, segmentSchemas);

    if (segmentSchemasChange != null) {
      changes.addChangeRequest(new SegmentSchemasChangeRequest(segmentSchemasChange));
    }
  }

  @Override
  public void removeSegmentSchemasForTask(String taskId)
  {
    log.info("Unannouncing task [%s].", taskId);
    taskSinkSchema.remove(taskId);
  }

  /**
   * Returns Future that lists the segment load/drop requests since given counter.
   */
  public ListenableFuture<ChangeRequestsSnapshot<DataSegmentChangeRequest>> getSegmentChangesSince(ChangeRequestHistory.Counter counter)
  {
    if (counter.getCounter() < 0) {
      synchronized (lock) {
        Iterable<DataSegmentChangeRequest> segments = Iterables.transform(
            segmentLookup.keySet(),
            new Function<DataSegment, DataSegmentChangeRequest>()
            {
              @Nullable
              @Override
              public SegmentChangeRequestLoad apply(DataSegment input)
              {
                return new SegmentChangeRequestLoad(input);
              }
            }
        );

        Iterable<DataSegmentChangeRequest> sinkSchema = Iterables.transform(
            taskSinkSchema.values(),
            new Function<SegmentSchemas, DataSegmentChangeRequest>()
            {
              @Override
              public SegmentSchemasChangeRequest apply(SegmentSchemas input)
              {
                return new SegmentSchemasChangeRequest(input);
              }
            }
        );
        Iterable<DataSegmentChangeRequest> changeRequestIterables = Iterables.concat(segments, sinkSchema);
        SettableFuture<ChangeRequestsSnapshot<DataSegmentChangeRequest>> future = SettableFuture.create();
        future.set(ChangeRequestsSnapshot.success(changes.getLastCounter(), Lists.newArrayList(changeRequestIterables)));
        return future;
      }
    } else {
      return changes.getRequestsSince(counter);
    }
  }

  private String makeServedSegmentPath()
  {
    // server.getName() is already in the zk path
    return makeServedSegmentPath(
        UUIDUtils.generateUuid(
            server.getHost(),
            server.getType().toString(),
            server.getTier(),
            DateTimes.nowUtc().toString()
        )
    );
  }

  private String makeServedSegmentPath(String zNode)
  {
    return ZKPaths.makePath(liveSegmentLocation, StringUtils.format("%s%s", zNode, counter.getAndIncrement()));
  }

  private class SegmentZNode implements Comparable<SegmentZNode>
  {
    private final String path;

    private byte[] bytes = new byte[]{};
    private int count = 0;

    public SegmentZNode(String path)
    {
      this.path = path;
    }

    public String getPath()
    {
      return path;
    }

    public int getCount()
    {
      return count;
    }

    public byte[] getBytes()
    {
      return bytes;
    }

    public Set<DataSegment> getSegments()
    {
      if (bytes.length == 0) {
        return new HashSet<>();
      }
      try {
        return jsonMapper.readValue(
            bytes,
            new TypeReference<Set<DataSegment>>()
            {
            }
        );
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void addSegment(DataSegment segment)
    {
      Set<DataSegment> zkSegments = getSegments();
      zkSegments.add(segment);

      try {
        bytes = jsonMapper.writeValueAsBytes(zkSegments);
      }
      catch (Exception e) {
        zkSegments.remove(segment);
        throw new RuntimeException(e);
      }

      count++;
    }

    public void addSegments(Set<DataSegment> segments)
    {
      Set<DataSegment> zkSegments = getSegments();
      zkSegments.addAll(segments);

      try {
        bytes = jsonMapper.writeValueAsBytes(zkSegments);
      }
      catch (Exception e) {
        zkSegments.removeAll(segments);
        throw new RuntimeException(e);
      }

      count += segments.size();
    }

    public void removeSegment(DataSegment segment)
    {
      Set<DataSegment> zkSegments = getSegments();
      zkSegments.remove(segment);

      try {
        bytes = jsonMapper.writeValueAsBytes(zkSegments);
      }
      catch (Exception e) {
        zkSegments.add(segment);
        throw new RuntimeException(e);
      }

      count--;
    }

    @Override
    public boolean equals(Object o)
    {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      SegmentZNode that = (SegmentZNode) o;

      if (!path.equals(that.path)) {
        return false;
      }

      return true;
    }

    @Override
    public int hashCode()
    {
      return path.hashCode();
    }

    @Override
    public int compareTo(SegmentZNode segmentZNode)
    {
      return path.compareTo(segmentZNode.getPath());
    }
  }
}
