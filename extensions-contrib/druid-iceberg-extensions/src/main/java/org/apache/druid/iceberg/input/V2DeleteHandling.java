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

package org.apache.druid.iceberg.input;

import com.fasterxml.jackson.annotation.JsonValue;

/**
 * Controls how Druid handles Iceberg v2 delete files during ingestion.
 *
 * Iceberg v2 tables can contain positional delete files and equality delete files
 * alongside data files. This enum determines the behavior when such delete files
 * are encountered.
 */
public enum V2DeleteHandling
{
  /**
   * Default behavior. Ignores delete files entirely and reads only data files.
   * This preserves backward compatibility with v1-only behavior but silently
   * includes logically deleted rows when reading v2 tables with active deletes.
   */
  SKIP("skip"),

  /**
   * Uses Iceberg's native reader stack to apply positional and equality deletes
   * at read time. Only non-deleted rows are emitted. This bypasses warehouseSource
   * and uses Iceberg's own FileIO for data access.
   */
  APPLY("apply"),

  /**
   * Fails with an error if any FileScanTask contains delete files. Useful for
   * detecting v2 tables that have active deletes, without silently ingesting
   * incorrect data.
   */
  FAIL("fail");

  private final String value;

  V2DeleteHandling(final String value)
  {
    this.value = value;
  }

  @JsonValue
  public String getValue()
  {
    return value;
  }
}
