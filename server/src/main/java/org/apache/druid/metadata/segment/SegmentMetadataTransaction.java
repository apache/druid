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

import org.apache.druid.timeline.DataSegment;
import org.skife.jdbi.v2.Handle;

import javax.annotation.Nullable;

/**
 * Represents a single transaction involving read/write of segment metadata into
 * the metadata store. A transaction is associated with a single instance of a
 * {@link Handle} and is meant to be short-lived.
 * <p>
 * A transaction CANNOT read back records it has written due to rollback
 * restrictions in {@link CachedSegmentMetadataTransaction}.
 */
public interface SegmentMetadataTransaction
    extends SegmentMetadataReadTransaction, DatasourceSegmentMetadataWriter
{
  /**
   * Marks this transaction to be rolled back.
   */
  void setRollbackOnly();

  /**
   * Updates the payload of the given segment in the metadata store.
   * This method is used only by legacy tasks "move", "archive" and "restore".
   *
   * @return true if the segment payload was updated successfully, false otherwise
   */
  boolean updateSegmentPayload(DataSegment segment);

  @FunctionalInterface
  interface Callback<T>
  {
    @Nullable
    T inTransaction(SegmentMetadataTransaction transaction) throws Exception;
  }
}
