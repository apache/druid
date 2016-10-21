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

package io.druid.storage.hdfs;

import com.google.inject.Inject;

import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentKiller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsDataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(HdfsDataSegmentKiller.class);

  private static final String PATH_KEY = "path";

  private final Configuration config;

  @Inject
  public HdfsDataSegmentKiller(final Configuration config)
  {
    this.config = config;
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    final Path path = getPath(segment);
    log.info("killing segment[%s] mapped to path[%s]", segment.getIdentifier(), path);

    try {
      if (path.getName().endsWith(".zip")) {

        final FileSystem fs = path.getFileSystem(config);

        if (!fs.exists(path)) {
          log.warn("Segment Path [%s] does not exist. It appears to have been deleted already.", path);
          return ;
        }

        // path format -- > .../dataSource/interval/version/partitionNum/xxx.zip
        Path partitionNumDir = path.getParent();
        if (!fs.delete(partitionNumDir, true)) {
          throw new SegmentLoadingException(
              "Unable to kill segment, failed to delete dir [%s]",
              partitionNumDir.toString()
          );
        }

        //try to delete other directories if possible
        Path versionDir = partitionNumDir.getParent();
        if (safeNonRecursiveDelete(fs, versionDir)) {
          Path intervalDir = versionDir.getParent();
          if (safeNonRecursiveDelete(fs, intervalDir)) {
            Path dataSourceDir = intervalDir.getParent();
            safeNonRecursiveDelete(fs, dataSourceDir);
          }
        }
      } else {
        throw new SegmentLoadingException("Unknown file type[%s]", path);
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Unable to kill segment");
    }
  }

  private boolean safeNonRecursiveDelete(FileSystem fs, Path path)
  {
    try {
      return fs.delete(path, false);
    }
    catch (Exception ex) {
      return false;
    }
  }

  private Path getPath(DataSegment segment)
  {
    return new Path(String.valueOf(segment.getLoadSpec().get(PATH_KEY)));
  }
}
