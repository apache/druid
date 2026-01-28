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

package org.apache.druid.storage;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.io.File;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface StorageConnectorProvider
{
  /**
   * Returns the storage connector. Takes a parameter defaultTempDir to be possibly used as the temporary directory, if the
   * storage connector requires one. This StorageConnectorProvider is not guaranteed to use this value, even if the
   * StorageConnectorProvider requires one, as it gives priority to a value of defaultTempDir configured as a runtime
   * configuration.
   * <br>
   * This value needs to be passed instead of injected by Jackson as the default temporary directory is dependent on the
   * task id, and such dynamic task specific bindings is not possible on indexers.
   */
  StorageConnector createStorageConnector(File defaultTempDir);
}
