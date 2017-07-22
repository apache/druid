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

package io.druid.storage.cassandra;

import com.google.common.base.Predicates;
import com.google.inject.Inject;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;
import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.FileUtils;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.RetryUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.loading.DataSegmentPuller;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.Callable;

/**
 * Cassandra Segment Puller
 */
public class CassandraDataSegmentPuller extends CassandraStorage implements DataSegmentPuller
{
  private static final Logger log = new Logger(CassandraDataSegmentPuller.class);
  private static final int CONCURRENCY = 10;
  private static final int BATCH_SIZE = 10;

  @Inject
  public CassandraDataSegmentPuller(CassandraDataSegmentConfig config)
  {
    super(config);
  }

  @Override
  public void getSegmentFiles(DataSegment segment, File outDir) throws SegmentLoadingException
  {
    String key = (String) segment.getLoadSpec().get("key");
    getSegmentFiles(key, outDir);
  }
  public FileUtils.FileCopyResult getSegmentFiles(final String key, final File outDir)
      throws SegmentLoadingException
  {
    log.info("Pulling index from C* at path[%s] to outDir[%s]", key, outDir);
    if (!outDir.exists()) {
      outDir.mkdirs();
    }

    if (!outDir.isDirectory()) {
      throw new ISE("outDir[%s] must be a directory.", outDir);
    }

    long startTime = System.currentTimeMillis();
    final File tmpFile = new File(outDir, "index.zip");
    log.info("Pulling to temporary local cache [%s]", tmpFile.getAbsolutePath());

    final FileUtils.FileCopyResult localResult;
    try {
      localResult = RetryUtils.retry(
          new Callable<FileUtils.FileCopyResult>()
          {
            @Override
            public FileUtils.FileCopyResult call() throws Exception
            {
              try (OutputStream os = new FileOutputStream(tmpFile)) {
                final ObjectMetadata meta = ChunkedStorage
                    .newReader(indexStorage, key, os)
                    .withBatchSize(BATCH_SIZE)
                    .withConcurrencyLevel(CONCURRENCY)
                    .call();
              }
              return new FileUtils.FileCopyResult(tmpFile);
            }
          },
          Predicates.<Throwable>alwaysTrue(),
          10
      );
    }
    catch (Exception e) {
      throw new SegmentLoadingException(e, "Unable to copy key [%s] to file [%s]", key, tmpFile.getAbsolutePath());
    }
    try{
    final FileUtils.FileCopyResult result =  CompressionUtils.unzip(tmpFile, outDir);
      log.info(
          "Pull of file[%s] completed in %,d millis (%s bytes)", key, System.currentTimeMillis() - startTime,
          result.size()
      );
      return result;
    }
    catch (Exception e) {
      try {
        org.apache.commons.io.FileUtils.deleteDirectory(outDir);
      }
      catch (IOException e1) {
        log.error(e1, "Error clearing segment directory [%s]", outDir.getAbsolutePath());
        e.addSuppressed(e1);
      }
      throw new SegmentLoadingException(e, e.getMessage());
    }
    finally {
      if (!tmpFile.delete()) {
        log.warn("Could not delete cache file at [%s]", tmpFile.getAbsolutePath());
      }
    }
  }
}
