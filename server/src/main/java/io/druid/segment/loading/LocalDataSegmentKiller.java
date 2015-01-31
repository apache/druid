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

package io.druid.segment.loading;

import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.util.Map;

/**
 */
public class LocalDataSegmentKiller implements DataSegmentKiller
{
  private static final Logger log = new Logger(LocalDataSegmentKiller.class);

  @Override
  public void kill(DataSegment segment) throws SegmentLoadingException
  {
    final File path = getDirectory(segment);
    log.info("segment[%s] maps to path[%s]", segment.getIdentifier(), path);

    if (!path.isDirectory()) {
      if (!path.delete()) {
        log.error("Unable to delete file[%s].", path);
        throw new SegmentLoadingException("Couldn't kill segment[%s]", segment.getIdentifier());
      }

      return;
    }

    final File[] files = path.listFiles();
    int success = 0;

    for (File file : files) {
      if (!file.delete()) {
        log.error("Unable to delete file[%s].", file);
      } else {
        ++success;
      }
    }

    if (success == 0 && files.length != 0) {
      throw new SegmentLoadingException("Couldn't kill segment[%s]", segment.getIdentifier());
    }

    if (success < files.length) {
      log.warn("Couldn't completely kill segment[%s]", segment.getIdentifier());
    } else if (!path.delete()) {
      log.warn("Unable to delete directory[%s].", path);
      log.warn("Couldn't completely kill segment[%s]", segment.getIdentifier());
    }
  }

  private File getDirectory(DataSegment segment) throws SegmentLoadingException
  {
    final Map<String, Object> loadSpec = segment.getLoadSpec();
    final File path = new File(MapUtils.getString(loadSpec, "path"));

    if (!path.exists()) {
      throw new SegmentLoadingException("Asked to load path[%s], but it doesn't exist.", path);
    }

    return path.getParentFile();
  }
}
