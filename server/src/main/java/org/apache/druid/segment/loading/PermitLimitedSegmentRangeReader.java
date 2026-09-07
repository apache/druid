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

package org.apache.druid.segment.loading;

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * A {@link SegmentRangeReader} decorator that bounds concurrent deep-storage range reads: each {@link #readRange} call
 * acquires an on-demand-load permit from {@link StorageLoadingThreadPool#acquireLoadPermit()} and holds it for the
 * lifetime of the returned stream (released on close), so the permit spans exactly the wire transfer.
 */
public class PermitLimitedSegmentRangeReader implements SegmentRangeReader
{
  private final SegmentRangeReader delegate;
  private final StorageLoadingThreadPool pool;

  public PermitLimitedSegmentRangeReader(SegmentRangeReader delegate, StorageLoadingThreadPool pool)
  {
    this.delegate = delegate;
    this.pool = pool;
  }

  @Override
  public InputStream readRange(String filename, long offset, long length) throws IOException
  {
    final Closeable permit = pool.acquireLoadPermit();
    try {
      return new PermitReleasingInputStream(delegate.readRange(filename, offset, length), permit);
    }
    catch (Throwable t) {
      try {
        permit.close();
      }
      catch (Exception e) {
        t.addSuppressed(e);
      }
      throw t;
    }
  }

  /**
   * Releases the permit when the stream is closed (after the transfer completes or the reader aborts). The permit's
   * {@code close()} is idempotent, so a double-close of the stream over-releases nothing.
   */
  private static final class PermitReleasingInputStream extends FilterInputStream
  {
    private final Closeable permit;

    PermitReleasingInputStream(InputStream in, Closeable permit)
    {
      super(in);
      this.permit = permit;
    }

    @Override
    public void close() throws IOException
    {
      try {
        super.close();
      }
      finally {
        permit.close();
      }
    }
  }
}
