/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.storage.hdfs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.google.inject.Inject;

import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentFinder;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 */
public class HdfsDataSegmentFinder implements DataSegmentFinder
{

  private static final Logger log = new Logger(HdfsDataSegmentFinder.class);

  private final Configuration config;
  private final ObjectMapper mapper;

  @Inject
  public HdfsDataSegmentFinder(Configuration config, ObjectMapper mapper)
  {
    this.config = config;
    this.mapper = mapper;
  }

  @Override
  public Set<DataSegment> findSegments(String workingDirPathStr, boolean updateDescriptor)
      throws SegmentLoadingException
  {
    final Set<DataSegment> segments = Sets.newHashSet();
    final Path workingDirPath = new Path(workingDirPathStr);
    FileSystem fs;
    try {
      fs = workingDirPath.getFileSystem(config);

      log.info(fs.getScheme());
      log.info("FileSystem URI:" + fs.getUri().toString());

      if (!fs.exists(workingDirPath)) {
        throw new SegmentLoadingException("Working directory [%s] doesn't exist.", workingDirPath);
      }

      if (!fs.isDirectory(workingDirPath)) {
        throw new SegmentLoadingException("Working directory [%s] is not a directory!?", workingDirPath);
      }

      final RemoteIterator<LocatedFileStatus> it = fs.listFiles(workingDirPath, true);
      while (it.hasNext()) {
        final LocatedFileStatus locatedFileStatus = it.next();
        final Path path = locatedFileStatus.getPath();
        if (path.getName().equals("descriptor.json")) {
          final Path indexZip = new Path(path.getParent(), "index.zip");
          if (fs.exists(indexZip)) {
            final DataSegment dataSegment = mapper.readValue(fs.open(path), DataSegment.class);
            log.info("Found segment [%s] located at [%s]", dataSegment.getIdentifier(), indexZip);

            final Map<String, Object> loadSpec = dataSegment.getLoadSpec();
            final String pathWithoutScheme = indexZip.toUri().getPath();

            if (!loadSpec.get("type").equals(HdfsStorageDruidModule.SCHEME) || !loadSpec.get("path")
                                                                                        .equals(pathWithoutScheme)) {
              loadSpec.put("type", HdfsStorageDruidModule.SCHEME);
              loadSpec.put("path", pathWithoutScheme);
              if (updateDescriptor) {
                log.info("Updating loadSpec in descriptor.json at [%s] with new path [%s]", path, pathWithoutScheme);
                mapper.writeValue(fs.create(path, true), dataSegment);
              }
            }
            segments.add(dataSegment);
          } else {
            throw new SegmentLoadingException(
                "index.zip didn't exist at [%s] while descripter.json exists!?",
                indexZip
            );
          }
        }
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Problems interacting with filesystem[%s].", workingDirPath);
    }

    return segments;
  }

}
