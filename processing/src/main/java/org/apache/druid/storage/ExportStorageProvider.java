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
import com.google.inject.Provider;

import java.io.File;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface ExportStorageProvider extends Provider<StorageConnector>
{
  String getResourceType();

  /**
   * Return a URI representation of the base path. This is used to be used for logging and error messages.
   */
  String getBasePath();

  String getFilePathForManifest(String fileName);

  StorageConnector createStorageConnector(File taskTempDir);

  /**
   * It is the responsibilty of the caller to have the tempDir configured through runtime properties. If tempDir not configured,
   * usages of {@link StorageConnector} might throw exception.
   * <br></br>
   * Deprecated in favour of {@link ExportStorageProvider#createStorageConnector(File)}.
   */
  @Override
  @Deprecated
  default StorageConnector get()
  {
    return createStorageConnector(null);
  }
}
