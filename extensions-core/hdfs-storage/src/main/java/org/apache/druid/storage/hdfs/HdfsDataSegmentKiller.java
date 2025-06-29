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

package org.apache.druid.storage.hdfs;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.commons.lang3.StringUtils;
import org.apache.druid.guice.Hdfs;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.segment.loading.DataSegmentKiller;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.annotation.Nullable;
import java.io.IOException;

public class HdfsDataSegmentKiller implements DataSegmentKiller
{
  private static final EmittingLogger log = new EmittingLogger(HdfsDataSegmentKiller.class);

  private static final String PATH_KEY = "path";

  private final Configuration config;

  @Nullable
  private final Path storageDirectory;

  @Inject
  public HdfsDataSegmentKiller(@Hdfs final Configuration config, final HdfsDataSegmentPusherConfig pusherConfig)
  {
    this.config = config;

    if (!Strings.isNullOrEmpty(pusherConfig.getStorageDirectory())) {
      this.storageDirectory = new Path(pusherConfig.getStorageDirectory());
    } else {
      this.storageDirectory = null;
    }
  }

  private static Path getPath(DataSegment segment)
  {
    return new Path(String.valueOf(segment.getLoadSpec().get(PATH_KEY)));
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    final Path segmentPath = getPath(segment);
    log.info("Killing segment[%s] mapped to path[%s]", segment.getId(), segmentPath);

    try (final FileSystem fs = segmentPath.getFileSystem(config)) {
      String filename = segmentPath.getName();
      if (!filename.endsWith(".zip")) {
        throw new SegmentLoadingException("Unknown file type[%s]", segmentPath);
      } else {

        if (!fs.exists(segmentPath)) {
          log.warn("Segment path [%s] does not exist", segmentPath);
          return;
        }

        // There are 3 supported path formats:
        //    - hdfs://nn1/hdfs_base_directory/data_source_name/interval/version/shardNum/index.zip
        //    - hdfs://nn1/hdfs_base_directory/data_source_name/interval/version/shardNum_index.zip
        //    - hdfs://nn1/hdfs_base_directory/data_source_name/interval/version/shardNum_UUID_index.zip
        String[] zipParts = filename.split("_");

        Path descriptorPath = new Path(segmentPath.getParent(), "descriptor.json");
        if (zipParts.length > 1) {
          Preconditions.checkState(zipParts.length <= 3 &&
                                   StringUtils.isNumeric(zipParts[0]) &&
                                   "index.zip".equals(zipParts[zipParts.length - 1]),
                                   "Unexpected segmentPath format [%s]", segmentPath
          );

          descriptorPath = new Path(
              segmentPath.getParent(),
              org.apache.druid.java.util.common.StringUtils.format(
                  "%s_%sdescriptor.json",
                  zipParts[0],
                  zipParts.length == 2 ? "" : zipParts[1] + "_"
              )
          );
        }

        if (!fs.delete(segmentPath, false)) {
          throw new SegmentLoadingException("Unable to kill segment, failed to delete [%s]", segmentPath.toString());
        }

        // descriptor.json is a file to store segment metadata in deep storage. This file is deprecated and not stored
        // anymore, but we still delete them if exists.
        fs.delete(descriptorPath, false);

        removeEmptyParentDirectories(fs, segmentPath, zipParts.length > 1 ? 2 : 3);
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Unable to kill segment");
    }
  }

  @Override
  public void killAll() throws IOException
  {
    if (storageDirectory == null) {
      throw new ISE("Cannot delete all segment files since druid.storage.storageDirectory is not set.");
    }

    log.info("Deleting all segment files from hdfs dir [%s].", storageDirectory.toUri().toString());
    final FileSystem fs = storageDirectory.getFileSystem(config);
    fs.delete(storageDirectory, true);
  }

  private void removeEmptyParentDirectories(final FileSystem fs, final Path segmentPath, final int depth)
  {
    Path path = segmentPath;
    try {
      for (int i = 1; i <= depth; i++) {
        path = path.getParent();
        if (fs.listStatus(path).length != 0 || !fs.delete(path, false)) {
          break;
        }
      }
    }
    catch (Exception e) {
      log.makeAlert(e, "uncaught exception during segment killer").emit();
    }
  }
}
