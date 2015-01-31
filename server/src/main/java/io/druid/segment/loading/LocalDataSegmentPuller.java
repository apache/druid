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

import com.google.common.io.Files;
import com.metamx.common.MapUtils;
import com.metamx.common.logger.Logger;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.util.Map;

/**
 */
public class LocalDataSegmentPuller implements DataSegmentPuller
{
  private static final Logger log = new Logger(LocalDataSegmentPuller.class);

  @Override
  public void getSegmentFiles(DataSegment segment, File dir) throws SegmentLoadingException
  {
    final File path = getFile(segment);

    if (path.isDirectory()) {
      if (path.equals(dir)) {
        log.info("Asked to load [%s] into itself, done!", dir);
        return;
      }

      log.info("Copying files from [%s] to [%s]", path, dir);
      File file = null;
      try {
        final File[] files = path.listFiles();
        for (int i = 0; i < files.length; ++i) {
          file = files[i];
          Files.copy(file, new File(dir, file.getName()));
        }
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Unable to copy file[%s].", file);
      }
    } else {
      if (!path.getName().endsWith(".zip")) {
        throw new SegmentLoadingException("File is not a zip file[%s]", path);
      }

      log.info("Unzipping local file[%s] to [%s]", path, dir);
      try {
        CompressionUtils.unzip(path, dir);
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Unable to unzip file[%s]", path);
      }
    }
  }

  private File getFile(DataSegment segment) throws SegmentLoadingException
  {
    final Map<String, Object> loadSpec = segment.getLoadSpec();
    final File path = new File(MapUtils.getString(loadSpec, "path"));

    if (!path.exists()) {
      throw new SegmentLoadingException("Asked to load path[%s], but it doesn't exist.", path);
    }

    return path;
  }
}
