/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package io.druid.server.coordination;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import io.druid.curator.announcement.Announcer;
import io.druid.server.initialization.ZkPathsConfig;
import io.druid.timeline.DataSegment;
import org.apache.curator.utils.ZKPaths;

import java.io.IOException;

public class SingleDataSegmentAnnouncer extends AbstractDataSegmentAnnouncer
{
  private static final Logger log = new Logger(SingleDataSegmentAnnouncer.class);

  private final Announcer announcer;
  private final ObjectMapper jsonMapper;
  private final String servedSegmentsLocation;

  @Inject
  public SingleDataSegmentAnnouncer(
      DruidServerMetadata server,
      ZkPathsConfig config,
      Announcer announcer,
      ObjectMapper jsonMapper
  )
  {
    super(server, config, announcer, jsonMapper);

    this.announcer = announcer;
    this.jsonMapper = jsonMapper;
    this.servedSegmentsLocation = ZKPaths.makePath(config.getServedSegmentsPath(), server.getName());
  }

  public void announceSegment(DataSegment segment) throws IOException
  {
    final String path = makeServedSegmentPath(segment);
    log.info("Announcing segment[%s] to path[%s]", segment.getIdentifier(), path);
    announcer.announce(path, jsonMapper.writeValueAsBytes(segment));
  }

  public void unannounceSegment(DataSegment segment) throws IOException
  {
    final String path = makeServedSegmentPath(segment);
    log.info("Unannouncing segment[%s] at path[%s]", segment.getIdentifier(), path);
    announcer.unannounce(path);
  }

  @Override
  public void announceSegments(Iterable<DataSegment> segments) throws IOException
  {
    for (DataSegment segment : segments) {
      announceSegment(segment);
    }
  }

  @Override
  public void unannounceSegments(Iterable<DataSegment> segments) throws IOException
  {
    for (DataSegment segment : segments) {
      unannounceSegment(segment);
    }
  }

  private String makeServedSegmentPath(DataSegment segment)
  {
    return ZKPaths.makePath(servedSegmentsLocation, segment.getIdentifier());
  }
}
