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

package io.druid.storage.hdfs;

import com.google.inject.Inject;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;

/**
 */
public class HdfsDataSegmentPuller implements DataSegmentPuller
{
  private final Configuration config;

  @Inject
  public HdfsDataSegmentPuller(final Configuration config)
  {
    this.config = config;
  }

  @Override
  public void getSegmentFiles(DataSegment segment, File dir) throws SegmentLoadingException
  {
    final Path path = getPath(segment);

    final FileSystem fs = checkPathAndGetFilesystem(path);

    if (path.getName().endsWith(".zip")) {
      try {
        try (FSDataInputStream in = fs.open(path)) {
          CompressionUtils.unzip(in, dir);
        }
      }
      catch (IOException e) {
        throw new SegmentLoadingException(e, "Some IOException");
      }
    } else {
      throw new SegmentLoadingException("Unknown file type[%s]", path);
    }
  }

  @Override
  public long getLastModified(DataSegment segment) throws SegmentLoadingException
  {
    Path path = getPath(segment);
    FileSystem fs = checkPathAndGetFilesystem(path);

    try {
      return fs.getFileStatus(path).getModificationTime();
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Problem loading status of path[%s]", path);
    }
  }

  private Path getPath(DataSegment segment)
  {
    return new Path(String.valueOf(segment.getLoadSpec().get("path")));
  }

  private FileSystem checkPathAndGetFilesystem(Path path) throws SegmentLoadingException
  {
    FileSystem fs;
    try {
      fs = path.getFileSystem(config);

      if (!fs.exists(path)) {
        throw new SegmentLoadingException("Path[%s] doesn't exist.", path);
      }

      return fs;
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Problems interacting with filesystem[%s].", path);
    }
  }
}
