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

package org.apache.druid.segment;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.asyncresource.AsyncResource;
import org.apache.druid.common.asyncresource.AsyncResources;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.segment.column.ColumnConfig;
import org.apache.druid.segment.file.CountingRangeReader;
import org.apache.druid.segment.file.PartialSegmentDownloadListener;
import org.apache.druid.segment.file.PartialSegmentFileMapperV10;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.jupiter.api.io.TempDir;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.Callable;

/**
 * Shared harness for {@link PartialQueryableIndexCursorFactory} tests: building the segment is left to each subclass
 * (a plain aggregate-projection segment vs. a clustered base table), but opening a partial index over it from a
 * {@link CountingRangeReader} and wiring the on-demand download executor is identical and lives here.
 */
abstract class PartialQueryableIndexCursorFactoryTestBase extends InitializedNullHandlingTest
{
  protected static final ColumnConfig COLUMN_CONFIG = ColumnConfig.DEFAULT;

  @TempDir
  protected File perTestTempDir;

  /**
   * Mount a fresh partial index over the (already built) segment via a {@link PartialSegmentFileMapperV10} backed by
   * {@code rangeReader}, into a per-test cache subdirectory named {@code cacheName}. Only the V10 header is read up
   * front; no internal column files are downloaded until a cursor requires them.
   */
  protected IndexAndMapper openIndex(CountingRangeReader rangeReader, String cacheName) throws IOException
  {
    final File cacheDir = new File(perTestTempDir, cacheName);
    FileUtils.mkdirp(cacheDir);
    final PartialSegmentFileMapperV10 mapper = PartialSegmentFileMapperV10.create(
        rangeReader,
        TestHelper.makeJsonMapper(),
        cacheDir,
        IndexIO.V10_FILE_NAME,
        Collections.emptyList(),
        PartialSegmentDownloadListener.NOOP
    );
    return new IndexAndMapper(
        new PartialQueryableIndex(mapper.getSegmentFileMetadata(), mapper, COLUMN_CONFIG),
        mapper
    );
  }

  protected static ListeningExecutorService directExec()
  {
    return MoreExecutors.listeningDecorator(MoreExecutors.newDirectExecutorService());
  }

  /**
   * A {@link PartialBundleAcquirer} whose holds are no-ops, submitting downloads to {@code downloadExec}. Used by tests
   * that don't care about the cache-layer hold lifecycle.
   */
  protected static PartialBundleAcquirer noOpAcquirer(ListeningExecutorService downloadExec)
  {
    return new PartialBundleAcquirer()
    {
      @Override
      public Closeable acquire(String bundleName)
      {
        return () -> {};
      }

      @Override
      public <T> AsyncResource<T> submitDownload(Callable<T> task)
      {
        return AsyncResources.fromFutureUnmanaged(downloadExec.submit(task));
      }
    };
  }

  protected record IndexAndMapper(PartialQueryableIndex index, PartialSegmentFileMapperV10 mapper)
      implements AutoCloseable
  {
    @Override
    public void close()
    {
      mapper.close();
    }
  }
}
