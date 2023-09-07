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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Set;

/**
 * Commit metadata for a dataSource. Used by
 * {@link IndexerMetadataStorageCoordinator#announceHistoricalSegments(Set, DataSourceMetadata, DataSourceMetadata)}
 * to provide metadata transactions for segment inserts.
 *
 * Two metadata instances can be added together, and any conflicts are resolved in favor of the right-hand side.
 * This means metadata can be partitioned.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "object", value = ObjectMetadata.class)
})
public interface DataSourceMetadata
{
  /**
   * Returns true if this instance should be considered a valid starting point for a new dataSource that has
   * no existing metadata.
   */
  boolean isValidStart();

  /**
   * As in {@link IndexerMetadataStorageCoordinator#announceHistoricalSegments}, this class can represent start and
   * end of a sequence.
   *
   * This method converts itself into the one for start of a sequence. Most implementations can simply return
   * {@code this}.
   */
  DataSourceMetadata asStartMetadata();

  /**
   * Returns true if any information present in this instance matches analogous information from "other" and
   * so they are conflict-free. In other words, "one.plus(two)" and "two.plus(one)" should return equal
   * instances if "one" matches "two".
   *
   * One simple way to implement this is to make it the same as "equals", although that doesn't allow for
   * partitioned metadata.
   *
   * Behavior is undefined if you pass in an instance of a different class from this one.
   *
   * @param other another instance
   *
   * @return true or false
   */
  boolean matches(DataSourceMetadata other);

  /**
   * Returns a copy of this instance with "other" merged in. Any conflicts should be resolved in favor of
   * information from "other".
   *
   * Behavior is undefined if you pass in an instance of a different class from this one.
   *
   * @param other another instance
   *
   * @return merged copy
   */
  DataSourceMetadata plus(DataSourceMetadata other);

  /**
   * Returns a copy of this instance with "other" subtracted.
   *
   * Behavior is undefined if you pass in an instance of a different class from this one.
   *
   * @param other another instance
   * @return subtracted copy
   */
  DataSourceMetadata minus(DataSourceMetadata other);
}
