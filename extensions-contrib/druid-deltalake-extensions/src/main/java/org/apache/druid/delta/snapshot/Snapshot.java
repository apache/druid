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

package org.apache.druid.delta.snapshot;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;

/**
 * Represents a snapshot of a Delta table that can be retrieved.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "latest", value = LatestSnapshot.class),
    @JsonSubTypes.Type(name = "version", value = VersionedSnapshot.class),
})
public interface Snapshot
{

  /**
   * Retrieves a snapshot of the given Delta table using the provided engine.
   *
   * @param table  the Delta table from which to retrieve the snapshot
   * @param engine the engine used to retrieve the snapshot
   * @return the snapshot of the Delta table, as determined by the implementation
   */
  io.delta.kernel.Snapshot getSnapshot(Table table, Engine engine);
}
