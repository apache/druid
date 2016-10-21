/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordination;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import io.druid.common.utils.UUIDUtils;
import io.druid.curator.announcement.Announcer;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.initialization.BatchDataSegmentAnnouncerConfig;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicLong;

/**
 */
public class BatchDataSegmentAnnouncer extends AbstractDataSegmentAnnouncer
{
  private static final Logger log = new Logger(BatchDataSegmentAnnouncer.class);

  private final BatchDataSegmentAnnouncerConfig config;
  private final Announcer announcer;
  private final ObjectMapper jsonMapper;
  private final String liveSegmentLocation;
  private final DruidServerMetadata server;

  private final Object lock = new Object();
  private final AtomicLong counter = new AtomicLong(0);

  private final Set<SegmentZNode> availableZNodes = new ConcurrentSkipListSet<SegmentZNode>();
  private final Map<DataSegment, SegmentZNode> segmentLookup = Maps.newConcurrentMap();
  private final Function<DataSegment, DataSegment> segmentTransformer;

  @Inject
  public BatchDataSegmentAnnouncer(
      DruidServerMetadata server,
      final BatchDataSegmentAnnouncerConfig config,
      ZkPathsConfig zkPaths,
      Announcer announcer,
      ObjectMapper jsonMapper
  )
  {
    super(server, zkPaths, announcer, jsonMapper);
    this.config = config;
    this.announcer = announcer;
    this.jsonMapper = jsonMapper;
    this.server = server;

    this.liveSegmentLocation = ZKPaths.makePath(zkPaths.getLiveSegmentsPath(), server.getName());
    segmentTransformer = new Function<DataSegment, DataSegment>()
    {
      @Override
      public DataSegment apply(DataSegment input)
      {
        DataSegment rv = input;
        if (config.isSkipDimensionsAndMetrics()) {
          rv = rv.withDimensions(null).withMetrics(null);
        }
        if (config.isSkipLoadSpec()) {
          rv = rv.withLoadSpec(null);
        }
        return rv;
      }
    };
  }

  @Override
  public void announceSegment(DataSegment segment) throws IOException
  {
    DataSegment toAnnounce = segmentTransformer.apply(segment);
    int newBytesLen = jsonMapper.writeValueAsBytes(toAnnounce).length;
    if (newBytesLen > config.getMaxBytesPerNode()) {
      throw new ISE("byte size %,d exceeds %,d", newBytesLen, config.getMaxBytesPerNode());
    }

    synchronized (lock) {
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
                toAnnounce.getIdentifier(),
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

        log.info("Announcing segment[%s] at new path[%s]", toAnnounce.getIdentifier(), availableZNode.getPath());
        announcer.announce(availableZNode.getPath(), availableZNode.getBytes());
        segmentLookup.put(toAnnounce, availableZNode);
        availableZNodes.add(availableZNode);
      }
    }
  }

  @Override
  public void unannounceSegment(DataSegment segment) throws IOException
  {
    final SegmentZNode segmentZNode = segmentLookup.remove(segment);
    if (segmentZNode == null) {
      log.warn("No path to unannounce segment[%s]", segment.getIdentifier());
      return;
    }

    synchronized (lock) {
      segmentZNode.removeSegment(segment);

      log.info("Unannouncing segment[%s] at path[%s]", segment.getIdentifier(), segmentZNode.getPath());
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
    Iterable<DataSegment> toAnnounce = Iterables.transform(segments, segmentTransformer);
    SegmentZNode segmentZNode = new SegmentZNode(makeServedSegmentPath());
    Set<DataSegment> batch = Sets.newHashSet();
    int byteSize = 0;
    int count = 0;

    for (DataSegment segment : toAnnounce) {
      int newBytesLen = jsonMapper.writeValueAsBytes(segment).length;

      if (newBytesLen > config.getMaxBytesPerNode()) {
        throw new ISE("byte size %,d exceeds %,d", newBytesLen, config.getMaxBytesPerNode());
      }

      if (count >= config.getSegmentsPerNode() || byteSize + newBytesLen > config.getMaxBytesPerNode()) {
        segmentZNode.addSegments(batch);
        announcer.announce(segmentZNode.getPath(), segmentZNode.getBytes());

        segmentZNode = new SegmentZNode(makeServedSegmentPath());
        batch = Sets.newHashSet();
        count = 0;
        byteSize = 0;
      }

      log.info("Announcing segment[%s] at path[%s]", segment.getIdentifier(), segmentZNode.getPath());
      segmentLookup.put(segment, segmentZNode);
      batch.add(segment);
      count++;
      byteSize += newBytesLen;
    }

    segmentZNode.addSegments(batch);
    announcer.announce(segmentZNode.getPath(), segmentZNode.getBytes());
  }

  @Override
  public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
  {
    for (DataSegment segment : segments) {
      unannounceSegment(segment);
    }
  }

  @Override
  public boolean isAnnounced(DataSegment segment)
  {
    return segmentLookup.containsKey(segment);
  }

  private String makeServedSegmentPath()
  {
    // server.getName() is already in the zk path
    return makeServedSegmentPath(UUIDUtils.generateUuid(
        server.getHost(),
        server.getType(),
        server.getTier(),
        new DateTime().toString()
    ));
  }

  private String makeServedSegmentPath(String zNode)
  {
    return ZKPaths.makePath(liveSegmentLocation, String.format("%s%s", zNode, counter.getAndIncrement()));
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
        return Sets.newHashSet();
      }
      try {
        return jsonMapper.readValue(
            bytes, new TypeReference<Set<DataSegment>>()
            {
            }
        );
      }
      catch (Exception e) {
        throw Throwables.propagate(e);
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
        throw Throwables.propagate(e);
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
        throw Throwables.propagate(e);
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
        throw Throwables.propagate(e);
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
