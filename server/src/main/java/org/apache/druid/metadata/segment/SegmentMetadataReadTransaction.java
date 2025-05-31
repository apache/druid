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
import org.skife.jdbi.v2.Handle;

import java.io.Closeable;

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

  @FunctionalInterface
  interface Callback<T>
  {
    T inTransaction(SegmentMetadataReadTransaction transaction) throws Exception;
  }

}
