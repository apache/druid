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

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    final File path = getPath(segment);
    log.info("killing segment[%s] mapped to path[%s]", segment.getIdentifier(), path);

    try {
      if (path.getName().endsWith(".zip")) {

        // path format -- > .../dataSource/interval/version/partitionNum/xxx.zip
        File partitionNumDir = path.getParentFile();
        FileUtils.deleteDirectory(partitionNumDir);

        //try to delete other directories if possible
        File versionDir = partitionNumDir.getParentFile();
        if (versionDir.delete()) {
          File intervalDir = versionDir.getParentFile();
          if (intervalDir.delete()) {
            File dataSourceDir = intervalDir.getParentFile();
            dataSourceDir.delete();
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

  private File getPath(DataSegment segment) throws SegmentLoadingException
  {
    return new File(MapUtils.getString(segment.getLoadSpec(), PATH_KEY));
  }
}
