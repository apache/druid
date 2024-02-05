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

import org.apache.druid.error.DruidException;

import java.io.File;

public class StorageConnectorUtils
{
  public static File validateAndGetPath(String basePath, String customPath)
  {
    if (basePath == null) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.NOT_FOUND)
                          .build(
                              "The runtime property `druid.export.storage.baseDir` must be configured for export functionality.");
    }
    final File baseDir = new File(basePath);
    final File exportFile = new File(baseDir, customPath);
    if (!exportFile.toPath().normalize().startsWith(baseDir.toPath())) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "The provided destination must be within the path configured by runtime property `druid.export.storage.baseDir`");
    }
    return exportFile;
  }

  private StorageConnectorUtils()
  {
  }
}
