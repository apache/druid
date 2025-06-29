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

import com.google.inject.Inject;
import org.apache.druid.data.SearchableVersionedDataFinder;
import org.apache.druid.guice.Hdfs;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.regex.Pattern;

public class HdfsFileTimestampVersionFinder extends HdfsDataSegmentPuller implements SearchableVersionedDataFinder<URI>
{
  @Inject
  public HdfsFileTimestampVersionFinder(@Hdfs Configuration config)
  {
    super(config);
  }

  private URI mostRecentInDir(final Path dir, final Pattern pattern) throws IOException
  {
    final PathFilter filter = new PathFilter()
    {
      @Override
      public boolean accept(Path path)
      {
        return pattern == null || pattern.matcher(path.getName()).matches();
      }
    };
    long modifiedTime = Long.MIN_VALUE;
    URI mostRecentURI = null;
    try (final FileSystem fs = dir.getFileSystem(config)) {
      for (FileStatus status : fs.listStatus(dir, filter)) {
        if (status.isFile()) {
          final long thisModifiedTime = status.getModificationTime();
          if (thisModifiedTime >= modifiedTime) {
            modifiedTime = thisModifiedTime;
            mostRecentURI = status.getPath().toUri();
          }
        }
      }
    }
    return mostRecentURI;
  }

  /**
   * Returns the latest modified file at the uri of interest.
   *
   * @param uri     Either a directory or a file on HDFS. If it is a file, the parent directory will be searched.
   * @param pattern A pattern matcher for file names in the directory of interest. Passing `null` results in matching any file in the directory.
   *
   * @return The URI of the file with the most recent modified timestamp.
   */
  @Override
  public URI getLatestVersion(final URI uri, final @Nullable Pattern pattern)
  {
    final Path path = new Path(uri);
    try {
      return RetryUtils.retry(
          () -> {
            final FileSystem fs = path.getFileSystem(config);
            if (!fs.exists(path)) {
              return null;
            }
            return mostRecentInDir(fs.isDirectory(path) ? path : path.getParent(), pattern);
          },
          shouldRetryPredicate(),
          DEFAULT_RETRY_COUNT
      );
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}
