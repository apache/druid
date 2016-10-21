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

import com.google.common.base.Throwables;
import com.google.inject.Inject;

import io.druid.data.SearchableVersionedDataFinder;
import io.druid.java.util.common.RetryUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 * This is implemented explicitly for URIExtractionNamespaceFunctionFactory
 * If you have a use case for this interface beyond URIExtractionNamespaceFunctionFactory please bring it up in the dev list.
 */
public class HdfsFileTimestampVersionFinder extends HdfsDataSegmentPuller implements SearchableVersionedDataFinder<URI>
{
  @Inject
  public HdfsFileTimestampVersionFinder(Configuration config)
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
    final FileSystem fs = dir.getFileSystem(config);
    for (FileStatus status : fs.listStatus(dir, filter)) {
      if (status.isFile()) {
        final long thisModifiedTime = status.getModificationTime();
        if (thisModifiedTime >= modifiedTime) {
          modifiedTime = thisModifiedTime;
          mostRecentURI = status.getPath().toUri();
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
          new Callable<URI>()
          {
            @Override
            public URI call() throws Exception
            {
              final FileSystem fs = path.getFileSystem(config);
              if (!fs.exists(path)) {
                return null;
              }
              return mostRecentInDir(fs.isDirectory(path) ? path : path.getParent(), pattern);
            }
          },
          shouldRetryPredicate(),
          DEFAULT_RETRY_COUNT
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Class<URI> getDataDescriptorClass()
  {
    return URI.class;
  }
}
