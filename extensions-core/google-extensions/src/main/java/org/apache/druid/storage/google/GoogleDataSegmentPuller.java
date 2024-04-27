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

package org.apache.druid.storage.google;

import com.google.common.base.Predicate;
import com.google.inject.Inject;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.segment.loading.URIDataPuller;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class GoogleDataSegmentPuller implements URIDataPuller
{
  private static final Logger LOG = new Logger(GoogleDataSegmentPuller.class);

  protected final GoogleStorage storage;

  @Inject
  public GoogleDataSegmentPuller(final GoogleStorage storage)
  {
    this.storage = storage;
  }

  FileUtils.FileCopyResult getSegmentFiles(final String bucket, final String path, File outDir)
      throws SegmentLoadingException
  {
    LOG.info("Pulling index at bucket[%s] path[%s] to outDir[%s]", bucket, path, outDir.getAbsolutePath());

    try {
      FileUtils.mkdirp(outDir);

      final GoogleByteSource byteSource = new GoogleByteSource(storage, bucket, path);
      final FileUtils.FileCopyResult result = CompressionUtils.unzip(
          byteSource,
          outDir,
          GoogleUtils::isRetryable,
          false
      );
      LOG.info("Loaded %d bytes from [%s] to [%s]", result.size(), path, outDir.getAbsolutePath());
      return result;
    }
    catch (Exception e) {
      try {
        FileUtils.deleteDirectory(outDir);
      }
      catch (IOException ioe) {
        LOG.warn(
            ioe,
            "Failed to remove output directory [%s] for segment pulled from [%s]",
            outDir.getAbsolutePath(),
            path
        );
      }
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  @Override
  public InputStream getInputStream(URI uri) throws IOException
  {
    String path = StringUtils.maybeRemoveLeadingSlash(uri.getPath());
    return storage.getInputStream(uri.getHost() != null ? uri.getHost() : uri.getAuthority(), path);
  }

  @Override
  public String getVersion(URI uri) throws IOException
  {
    String path = StringUtils.maybeRemoveLeadingSlash(uri.getPath());
    return storage.version(uri.getHost() != null ? uri.getHost() : uri.getAuthority(), path);
  }

  @Override
  public Predicate<Throwable> shouldRetryPredicate()
  {
    return new Predicate<Throwable>()
    {
      @Override
      public boolean apply(Throwable e)
      {
        if (e == null) {
          return false;
        }
        if (GoogleUtils.isRetryable(e)) {
          return true;
        }
        // Look all the way down the cause chain, just in case something wraps it deep.
        return apply(e.getCause());
      }
    };
  }
}
