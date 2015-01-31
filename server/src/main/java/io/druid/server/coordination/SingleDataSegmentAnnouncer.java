/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
