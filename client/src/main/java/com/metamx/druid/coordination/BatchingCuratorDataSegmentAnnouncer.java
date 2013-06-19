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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.curator.SegmentReader;
import com.metamx.druid.curator.announcement.Announcer;
import com.metamx.druid.initialization.ZkPathsConfig;
import org.apache.curator.utils.ZKPaths;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 */
public class BatchingCuratorDataSegmentAnnouncer extends AbstractDataSegmentAnnouncer
{
  private static final Logger log = new Logger(BatchingCuratorDataSegmentAnnouncer.class);

  private final ZkPathsConfig config;
  private final Announcer announcer;
  private final ObjectMapper jsonMapper;
  private final SegmentReader segmentReader;
  private final String liveSegmentLocation;

  private final Map<String, Integer> zNodes = Maps.newHashMap();
  private final Map<String, String> segmentLookup = new ConcurrentHashMap<String, String>();

  public BatchingCuratorDataSegmentAnnouncer(
      DruidServerMetadata server,
      ZkPathsConfig config,
      Announcer announcer,
      ObjectMapper jsonMapper,
      SegmentReader segmentReader
  )
  {
    super(server, config, announcer, jsonMapper);

    this.config = config;
    this.announcer = announcer;
    this.jsonMapper = jsonMapper;
    this.segmentReader = segmentReader;
    this.liveSegmentLocation = ZKPaths.makePath(config.getLiveSegmentsPath(), server.getName());
  }

  @Override
  public void announceSegment(DataSegment segment) throws IOException
  {
    Map.Entry<String, Integer> zNode = (zNodes.entrySet().isEmpty()) ? null : zNodes.entrySet().iterator().next();

    final String path = (zNode == null) ? makeServedSegmentPath(new DateTime().toString()) : zNode.getKey();

    Set<DataSegment> zkSegments = segmentReader.read(path);
    zkSegments.add(segment);
    if (zkSegments.size() >= config.getSegmentsPerNode()) {
      zNodes.remove(path);
    } else {
      zNodes.put(path, zkSegments.size());
    }
    segmentLookup.put(segment.getIdentifier(), path);

    log.info("Announcing segment[%s] to path[%s]", segment.getIdentifier(), path);

    byte[] bytes = jsonMapper.writeValueAsBytes(zkSegments);
    if (bytes.length > config.getMaxNumBytes()) {
      throw new ISE("byte size %,d exceeds %,d", bytes.length, config.getMaxNumBytes());
    }

    announcer.update(path, bytes);
  }

  @Override
  public void unannounceSegment(DataSegment segment) throws IOException
  {
    final String path = segmentLookup.get(segment.getIdentifier());

    Set<DataSegment> zkSegments = segmentReader.read(path);
    zkSegments.remove(segment);

    log.info("Unannouncing segment[%s] at path[%s]", segment.getIdentifier(), path);
    if (zkSegments.isEmpty()) {
      announcer.unannounce(path);
    } else if (zkSegments.size() < config.getSegmentsPerNode()) {
      zNodes.put(path, zkSegments.size());
      announcer.update(path, jsonMapper.writeValueAsBytes(zkSegments));
    }
  }

  @Override
  public void announceSegments(Iterable<DataSegment> segments) throws IOException
  {
    Iterable<List<DataSegment>> batched = Iterables.partition(segments, config.getSegmentsPerNode());

    for (List<DataSegment> batch : batched) {
      final String path = makeServedSegmentPath(new DateTime().toString());
      for (DataSegment segment : batch) {
        log.info("Announcing segment[%s] to path[%s]", segment.getIdentifier(), path);
        segmentLookup.put(segment.getIdentifier(), path);
      }
      if (batch.size() < config.getSegmentsPerNode()) {
        zNodes.put(path, batch.size());
      }

      byte[] bytes = jsonMapper.writeValueAsBytes(batch);
      if (bytes.length > config.getMaxNumBytes()) {
        throw new ISE("byte size %,d exceeds %,d", bytes.length, config.getMaxNumBytes());
      }

      announcer.update(path, bytes);
    }
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
}
