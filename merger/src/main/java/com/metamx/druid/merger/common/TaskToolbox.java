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

package com.metamx.druid.merger.common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.loading.MMappedQueryableIndexFactory;
import com.metamx.druid.loading.S3DataSegmentPuller;
import com.metamx.druid.loading.SegmentLoadingException;
import com.metamx.druid.loading.SingleSegmentLoader;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.emitter.service.ServiceEmitter;

import org.jets3t.service.impl.rest.httpclient.RestS3Service;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * Stuff that may be needed by a Task in order to conduct its business.
 */
public class TaskToolbox
{
  private final IndexerCoordinatorConfig config;
  private final ServiceEmitter emitter;
  private final RestS3Service s3Client;
  private final DataSegmentPusher segmentPusher;
  private final ObjectMapper objectMapper;

  public TaskToolbox(
      IndexerCoordinatorConfig config,
      ServiceEmitter emitter,
      RestS3Service s3Client,
      DataSegmentPusher segmentPusher,
      ObjectMapper objectMapper
  )
  {
    this.config = config;
    this.emitter = emitter;
    this.s3Client = s3Client;
    this.segmentPusher = segmentPusher;
    this.objectMapper = objectMapper;
  }

  public IndexerCoordinatorConfig getConfig()
  {
    return config;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public RestS3Service getS3Client()
  {
    return s3Client;
  }

  public DataSegmentPusher getSegmentPusher()
  {
    return segmentPusher;
  }

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  public Map<DataSegment, File> getSegments(final Task task, List<DataSegment> segments)
      throws SegmentLoadingException
  {
    final SingleSegmentLoader loader = new SingleSegmentLoader(
        new S3DataSegmentPuller(s3Client),
        new MMappedQueryableIndexFactory(),
        new File(config.getTaskDir(task), "fetched_segments")
    );

    Map<DataSegment, File> retVal = Maps.newLinkedHashMap();
    for (DataSegment segment : segments) {
      retVal.put(segment, loader.getSegmentFiles(segment));
    }

    return retVal;
  }
}
