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

import com.google.common.base.Throwables;

import io.druid.data.SearchableVersionedDataFinder;
import io.druid.java.util.common.RetryUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.regex.Pattern;

/**
 * This is implemented explicitly for URIExtractionNamespaceFunctionFactory
 * If you have a use case for this interface beyond URIExtractionNamespaceFunctionFactory please bring it up in the dev list.
 */
public class LocalFileTimestampVersionFinder extends LocalDataSegmentPuller
    implements SearchableVersionedDataFinder<URI>
{
  public static final String URI_SCHEME = "file";
  private URI mostRecentInDir(final Path dir, final Pattern pattern) throws IOException
  {
    long latestModified = Long.MIN_VALUE;
    URI latest = null;
    for (File file : dir.toFile().listFiles(
        new FileFilter()
        {
          @Override
          public boolean accept(File pathname)
          {
            return pathname.exists()
                   && pathname.isFile()
                   && (pattern == null || pattern.matcher(pathname.getName()).matches());
          }
        }
    )) {
      final long thisModified = file.lastModified();
      if (thisModified >= latestModified) {
        latestModified = thisModified;
        latest = file.toURI();
      }
    }
    return latest;
  }

  /**
   * Matches based on a pattern in the file name. Returns the file with the latest timestamp.
   *
   * @param uri     If it is a file, then the parent is searched. If it is a directory, then the directory is searched.
   * @param pattern The matching filter to down-select the file names in the directory of interest. Passing `null` results in matching any file
   *
   * @return The URI of the most recently modified file which matches the pattern, or `null` if it cannot be found
   */
  @Override
  public URI getLatestVersion(URI uri, final @Nullable Pattern pattern)
  {
    final File file = new File(uri);
    try {
      return RetryUtils.retry(
          new Callable<URI>()
          {
            @Override
            public URI call() throws Exception
            {
              return mostRecentInDir(
                  file.isDirectory() ? file.toPath() : file.getParentFile().toPath(),
                  pattern
              );
            }
          },
          shouldRetryPredicate(),
          DEFAULT_RETRY_COUNT
      );
    }
    catch (Exception e) {
      if (e instanceof FileNotFoundException) {
        return null;
      }
      throw Throwables.propagate(e);
    }
  }

  @Override
  public Class<URI> getDataDescriptorClass()
  {
    return URI.class;
  }
}
