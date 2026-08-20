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

package org.apache.druid.segment.file;

/**
 * Sink for download events emitted by {@link PartialSegmentFileMapperV10} as it materializes internal files on demand
 * from deep storage, for collecting metrics.
 */
public interface PartialSegmentDownloadListener
{
  PartialSegmentDownloadListener NOOP = new PartialSegmentDownloadListener()
  {
    @Override
    public void onBytesDownloaded(long bytes)
    {
      // no-op
    }

    @Override
    public void onRangeRead(long bytes, long nanos)
    {
      // no-op
    }
  };

  /**
   * Bytes were newly materialized on local disk from deep storage and are now resident and usable: an internal file as
   * it is accessed (fired exactly once per file, from whichever download path first records it), or the V10 header on a
   * genuine cold mount (not a restore from a previous session's on-disk state). Distinct from
   * {@link #onRangeRead bytes pulled over the wire}, which can be larger when a partially-present container is re-read.
   *
   * @param bytes the materialized byte count
   */
  void onBytesDownloaded(long bytes);

  /**
   * A single deep-storage range read completed: the actual request granularity (one read may cover many files when a
   * whole container is fetched at once). Measures wire bytes and latency, so it reflects the deep-storage request count
   * and cost rather than how many files became resident.
   *
   * @param bytes the number of bytes read from deep storage in this request
   * @param nanos the wall-clock time the read took, in nanoseconds
   */
  void onRangeRead(long bytes, long nanos);
}
