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

package org.apache.druid.storage.cassandra;

import com.google.common.base.Predicates;
import com.google.inject.Inject;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.RetryUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Cassandra Segment Puller
 */
public class CassandraDataSegmentPuller extends CassandraStorage
{
  private static final Logger log = new Logger(CassandraDataSegmentPuller.class);
  private static final int CONCURRENCY = 10;
  private static final int BATCH_SIZE = 10;

  @Inject
  public CassandraDataSegmentPuller(CassandraDataSegmentConfig config)
  {
    super(config);
  }

  FileUtils.FileCopyResult getSegmentFiles(final String key, final File outDir) throws SegmentLoadingException
  {
    log.info("Pulling index from C* at path[%s] to outDir[%s]", key, outDir);
    try {
      org.apache.commons.io.FileUtils.forceMkdir(outDir);
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "");
    }

    long startTime = System.currentTimeMillis();
    final File tmpFile = new File(outDir, "index.zip");
    log.info("Pulling to temporary local cache [%s]", tmpFile.getAbsolutePath());

    try {
      RetryUtils.retry(
          () -> {
            try (OutputStream os = new FileOutputStream(tmpFile)) {
              ChunkedStorage
                  .newReader(indexStorage, key, os)
                  .withBatchSize(BATCH_SIZE)
                  .withConcurrencyLevel(CONCURRENCY)
                  .call();
            }
            return new FileUtils.FileCopyResult(tmpFile);
          },
          Predicates.alwaysTrue(),
          10
      );
    }
    catch (Exception e) {
      throw new SegmentLoadingException(e, "Unable to copy key [%s] to file [%s]", key, tmpFile.getAbsolutePath());
    }
    try {
      final FileUtils.FileCopyResult result = CompressionUtils.unzip(tmpFile, outDir);
      log.info(
          "Pull of file[%s] completed in %,d millis (%s bytes)", key, System.currentTimeMillis() - startTime,
          result.size()
      );
      return result;
    }
    catch (Exception e) {
      try {
        FileUtils.deleteDirectory(outDir);
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
