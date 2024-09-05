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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.delta.kernel.Table;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.KernelException;
import org.apache.druid.error.InvalidInput;

/**
 * Represents a Delta table snapshot identified by a specific version.
 */
public class VersionedSnapshot implements Snapshot
{
  @JsonProperty
  private final Long version;

  @JsonCreator
  public VersionedSnapshot(@JsonProperty("version") final Long version)
  {
    if (version == null) {
      throw InvalidInput.exception("version cannot be empty or null for versioned snapshot reads.");
    }
    this.version = version;
  }

  @Override
  public io.delta.kernel.Snapshot getSnapshot(Table table, Engine engine)
  {
    try {
      return table.getSnapshotAsOfVersion(engine, version);
    }
    catch (KernelException ke) {
      throw InvalidInput.exception(
          "Error reading snapshot version[%s] from tablePath[%s]: [%s]",
          version, table.getPath(engine), ke.getMessage()
      );
    }
  }
}
