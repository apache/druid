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

import com.google.inject.Inject;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;

/**
 */
public class LocalDataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(LocalDataSegmentKiller.class);

  private static final String PATH_KEY = "path";

  private final File storageDirectory;

  @Inject
  public LocalDataSegmentKiller(LocalDataSegmentPusherConfig config)
  {
    this.storageDirectory = config.getStorageDirectory();
  }

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    final File path = getPath(segment);
    log.info("killing segment[%s] mapped to path[%s]", segment.getIdentifier(), path);

    try {
      if (path.getName().endsWith(".zip")) {
        // path format -- > .../dataSource/interval/version/partitionNum/xxx.zip
        // or .../dataSource/interval/version/partitionNum/UUID/xxx.zip

        File parentDir = path.getParentFile();
        FileUtils.deleteDirectory(parentDir);

        // possibly recursively delete empty parent directories up to 'dataSource'
        parentDir = parentDir.getParentFile();
        int maxDepth = 4; // if for some reason there's no datasSource directory, stop recursing somewhere reasonable
        while (parentDir != null && --maxDepth >= 0) {
          if (!parentDir.delete() || segment.getDataSource().equals(parentDir.getName())) {
            break;
          }

          parentDir = parentDir.getParentFile();
        }
      } else {
        throw new SegmentLoadingException("Unknown file type[%s]", path);
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Unable to kill segment");
    }
  }

  @Override
  public void killAll() throws IOException
  {
    log.info("Deleting all segment files from local dir [%s].", storageDirectory.getAbsolutePath());
    FileUtils.deleteDirectory(storageDirectory);
  }

  private File getPath(DataSegment segment)
  {
    return new File(MapUtils.getString(segment.getLoadSpec(), PATH_KEY));
  }
}
