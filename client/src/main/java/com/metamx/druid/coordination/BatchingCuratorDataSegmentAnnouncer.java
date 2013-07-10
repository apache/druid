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

package com.metamx.druid.coordination;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.curator.announcement.Announcer;
import com.metamx.druid.initialization.ZkDataSegmentAnnouncerConfig;
import com.metamx.druid.initialization.ZkPathsConfig;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class BatchingCuratorDataSegmentAnnouncer extends AbstractDataSegmentAnnouncer
{
  private static final Logger log = new Logger(BatchingCuratorDataSegmentAnnouncer.class);

  private final ZkDataSegmentAnnouncerConfig config;
  private final Announcer announcer;
  private final ObjectMapper jsonMapper;
  private final String liveSegmentLocation;

  private final Set<SegmentZNode> availableZNodes = Sets.newHashSet();
  private final Map<DataSegment, SegmentZNode> segmentLookup = Maps.newHashMap();

  public BatchingCuratorDataSegmentAnnouncer(
      DruidServerMetadata server,
      ZkDataSegmentAnnouncerConfig config,
      Announcer announcer,
      ObjectMapper jsonMapper
  )
  {
    super(server, config, announcer, jsonMapper);

    this.config = config;
    this.announcer = announcer;
    this.jsonMapper = jsonMapper;
    this.liveSegmentLocation = ZKPaths.makePath(config.getLiveSegmentsPath(), server.getName());
  }

  @Override
  public void announceSegment(DataSegment segment) throws IOException
  {
    int newBytesLen = jsonMapper.writeValueAsBytes(segment).length;
    if (newBytesLen > config.getMaxNumBytes()) {
      throw new ISE("byte size %,d exceeds %,d", newBytesLen, config.getMaxNumBytes());
    }

    // create new batch
    if (availableZNodes.isEmpty()) {
      SegmentZNode availableZNode = new SegmentZNode(makeServedSegmentPath(new DateTime().toString()));
      availableZNode.addSegment(segment);

      log.info("Announcing segment[%s] at path[%s]", segment.getIdentifier(), availableZNode.getPath());
      announcer.announce(availableZNode.getPath(), availableZNode.getBytes());
      segmentLookup.put(segment, availableZNode);
      availableZNodes.add(availableZNode);
    } else { // update existing batch
      Iterator<SegmentZNode> iter = availableZNodes.iterator();
      boolean done = false;
      while (iter.hasNext() && !done) {
        SegmentZNode availableZNode = iter.next();
        if (availableZNode.getBytes().length + newBytesLen < config.getMaxNumBytes()) {
          availableZNode.addSegment(segment);

          log.info("Announcing segment[%s] at path[%s]", segment.getIdentifier(), availableZNode.getPath());
          announcer.update(availableZNode.getPath(), availableZNode.getBytes());
          segmentLookup.put(segment, availableZNode);

          if (availableZNode.getCount() >= config.getSegmentsPerNode()) {
            availableZNodes.remove(availableZNode);
          }

          done = true;
        }
      }
    }
  }

  @Override
  public void unannounceSegment(DataSegment segment) throws IOException
  {
    final SegmentZNode segmentZNode = segmentLookup.remove(segment);
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

  @Override
  public void announceSegments(Iterable<DataSegment> segments) throws IOException
  {
    SegmentZNode segmentZNode = new SegmentZNode(makeServedSegmentPath(new DateTime().toString()));
    Set<DataSegment> batch = Sets.newHashSet();
    int byteSize = 0;
    int count = 0;

    for (DataSegment segment : segments) {
      int newBytesLen = jsonMapper.writeValueAsBytes(segment).length;

      if (newBytesLen > config.getMaxNumBytes()) {
        throw new ISE("byte size %,d exceeds %,d", newBytesLen, config.getMaxNumBytes());
      }

      if (count >= config.getSegmentsPerNode() || byteSize + newBytesLen > config.getMaxNumBytes()) {
        segmentZNode.addSegments(batch);
        announcer.announce(segmentZNode.getPath(), segmentZNode.getBytes());

        segmentZNode = new SegmentZNode(makeServedSegmentPath(new DateTime().toString()));
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

  private String makeServedSegmentPath(String zNode)
  {
    return ZKPaths.makePath(liveSegmentLocation, zNode);
  }

  private class SegmentZNode
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
  }
}
