/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

  @Override
  public long getLastModified(DataSegment segment) throws SegmentLoadingException
  {
    final File file = getFile(segment);

    long lastModified = Long.MAX_VALUE;
    if (file.isDirectory()) {
      for (File childFile : file.listFiles()) {
        lastModified = Math.min(childFile.lastModified(), lastModified);
      }
    }
    else {
      lastModified = file.lastModified();
    }

    return lastModified;
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
