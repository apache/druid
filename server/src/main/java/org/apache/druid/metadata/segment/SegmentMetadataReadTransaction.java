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

package org.apache.druid.metadata.segment;

import org.apache.druid.metadata.SqlSegmentsMetadataQuery;
import org.apache.druid.server.http.DataSegmentPlus;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.skife.jdbi.v2.Handle;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.List;
import java.util.Set;

/**
 * Represents a single transaction involving read of segment metadata into
 * the metadata store. A transaction is associated with a single instance of a
 * {@link Handle} and is meant to be short-lived.
 */
public interface SegmentMetadataReadTransaction
    extends DatasourceSegmentMetadataReader, Closeable
{
  /**
   * @return The JDBI handle used in this transaction
   */
  Handle getHandle();

  /**
   * @return SQL tool to read or update the metadata store directly.
   */
  SqlSegmentsMetadataQuery noCacheSql();

  /**
   * Completes the transaction by either committing it or rolling it back.
   */
  @Override
  void close();

  // Methods that can be served only partially by the cache

  /**
   * Retrieves the IDs of segments (out of the given set) which already exist in
   * the metadata store.
   */
  Set<String> findExistingSegmentIds(Set<SegmentId> segments);

  /**
   * Retrieves the segment for the given segment ID.
   *
   * @return null if no such segment exists in the metadata store.
   */
  @Nullable
  DataSegment findSegment(SegmentId segmentId);

  /**
   * Retrieves segments for the given segment IDs.
   */
  List<DataSegmentPlus> findSegments(
      Set<SegmentId> segmentIds
  );

  /**
   * Retrieves segments with additional metadata info such as number of rows and
   * schema fingerprint for the given segment IDs.
   */
  List<DataSegmentPlus> findSegmentsWithSchema(
      Set<SegmentId> segmentIds
  );

  @FunctionalInterface
  interface Callback<T>
  {
    T inTransaction(SegmentMetadataReadTransaction transaction) throws Exception;
  }

}
