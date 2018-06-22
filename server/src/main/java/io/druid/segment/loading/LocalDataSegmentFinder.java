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

package io.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.druid.guice.LocalDataStorageDruidModule;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 */
public class LocalDataSegmentFinder implements DataSegmentFinder
{
  private static final Logger log = new Logger(LocalDataSegmentFinder.class);

  private final ObjectMapper mapper;

  @Inject
  public LocalDataSegmentFinder(ObjectMapper mapper)
  {
    this.mapper = mapper;
  }

  @Override
  public Set<DataSegment> findSegments(String workingDirPath, boolean updateDescriptor) throws SegmentLoadingException
  {
    final Map<String, Pair<DataSegment, Long>> timestampedSegments = new HashMap<>();

    final File workingDir = new File(workingDirPath);
    if (!workingDir.isDirectory()) {
      throw new SegmentLoadingException("Working directory [%s] didn't exist !?", workingDir);
    }
    recursiveSearchSegments(timestampedSegments, workingDir, updateDescriptor);

    return timestampedSegments.values().stream().map(x -> x.lhs).collect(Collectors.toSet());
  }

  private void recursiveSearchSegments(
      Map<String, Pair<DataSegment, Long>> timestampedSegments, File workingDir, boolean updateDescriptor
  ) throws SegmentLoadingException
  {
    for (File file : workingDir.listFiles()) {
      if (file.isDirectory()) {
        recursiveSearchSegments(timestampedSegments, file, updateDescriptor);
      } else if (file.getName().equals("descriptor.json")) {
        final File indexZip = new File(file.getParentFile(), "index.zip");
        if (indexZip.exists()) {
          try {
            final DataSegment dataSegment = mapper.readValue(FileUtils.readFileToString(file), DataSegment.class);
            log.info("Found segment [%s] located at [%s]", dataSegment.getIdentifier(), indexZip.getAbsoluteFile());
            final Map<String, Object> loadSpec = dataSegment.getLoadSpec();
            if (!loadSpec.get("type").equals(LocalDataStorageDruidModule.SCHEME) || !loadSpec.get("path")
                                                                                             .equals(indexZip.getAbsoluteFile())) {
              loadSpec.put("type", LocalDataStorageDruidModule.SCHEME);
              loadSpec.put("path", indexZip.getAbsolutePath());
              if (updateDescriptor) {
                log.info(
                    "Updating loadSpec in descriptor.json at [%s] with new path [%s]",
                    file.getAbsolutePath(),
                    indexZip.toString()
                );
                FileUtils.writeStringToFile(file, mapper.writeValueAsString(dataSegment));
              }
            }

            DataSegmentFinder.putInMapRetainingNewest(timestampedSegments, dataSegment, indexZip.lastModified());
          }
          catch (IOException e) {
            throw new SegmentLoadingException(
                e,
                "Failed to read descriptor.json for segment located at [%s]",
                file.getAbsoluteFile()
            );
          }
        } else {
          throw new SegmentLoadingException(
              "index.zip didn't exist at [%s] while descripter.json exists!?",
              indexZip.getAbsoluteFile()
          );
        }
      }
    }
  }
}
