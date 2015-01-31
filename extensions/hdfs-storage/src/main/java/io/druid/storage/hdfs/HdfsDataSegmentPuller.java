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
