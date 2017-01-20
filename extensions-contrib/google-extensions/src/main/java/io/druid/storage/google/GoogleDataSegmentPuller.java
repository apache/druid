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

package io.druid.storage.google;

import com.google.common.base.Predicate;
import com.google.inject.Inject;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.MapUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.segment.loading.URIDataPuller;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;

public class GoogleDataSegmentPuller implements DataSegmentPuller, URIDataPuller
{
  private static final Logger LOG = new Logger(GoogleDataSegmentPuller.class);

  private final GoogleStorage storage;

  @Inject
  public GoogleDataSegmentPuller(final GoogleStorage storage)
  {
    this.storage = storage;
  }

  @Override
  public void getSegmentFiles(final DataSegment segment, final File outDir) throws SegmentLoadingException
  {
    final Map<String, Object> loadSpec = segment.getLoadSpec();
    final String bucket = MapUtils.getString(loadSpec, "bucket");
    final String path = MapUtils.getString(loadSpec, "path");

    getSegmentFiles(bucket, path, outDir);
  }

  public FileUtils.FileCopyResult getSegmentFiles(final String bucket, final String path, File outDir)
      throws SegmentLoadingException
  {
    LOG.info("Pulling index at path[%s] to outDir[%s]", bucket, path, outDir.getAbsolutePath());

    prepareOutDir(outDir);

    try {
      final GoogleByteSource byteSource = new GoogleByteSource(storage, bucket, path);
      final FileUtils.FileCopyResult result = CompressionUtils.unzip(
          byteSource,
          outDir,
          GoogleUtils.GOOGLE_RETRY,
          true
      );
      LOG.info("Loaded %d bytes from [%s] to [%s]", result.size(), path, outDir.getAbsolutePath());
      return result;
    }
    catch (Exception e) {
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(outDir);
      }
      catch (IOException ioe) {
        LOG.warn(
            ioe, "Failed to remove output directory [%s] for segment pulled from [%s]",
            outDir.getAbsolutePath(), path
        );
      }
      throw new SegmentLoadingException(e, e.getMessage());
    }
  }

  // Needs to be public for the tests.
  public void prepareOutDir(final File outDir) throws ISE
  {
    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    if (!outDir.isDirectory()) {
      throw new ISE("outDir[%s] must be a directory.", outDir);
    }
  }

  @Override
  public InputStream getInputStream(URI uri) throws IOException
  {
    String path = uri.getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return storage.get(uri.getHost(), path);
  }

  @Override
  public String getVersion(URI uri) throws IOException
  {
    String path = uri.getPath();
    if (path.startsWith("/")) {
      path = path.substring(1);
    }
    return storage.version(uri.getHost(), path);
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
        if (GoogleUtils.GOOGLE_RETRY.apply(e)) {
          return true;
        }
        // Look all the way down the cause chain, just in case something wraps it deep.
        return apply(e.getCause());
      }
    };
  }
}
