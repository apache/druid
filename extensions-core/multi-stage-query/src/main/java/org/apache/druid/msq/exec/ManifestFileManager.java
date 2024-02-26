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

package org.apache.druid.msq.exec;

import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.storage.StorageConnector;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Set;

/**
 * Manages reading and writing to the manifest file while exporting.
 */
public class ManifestFileManager
{
  private static final Logger log = new Logger(ManifestFileManager.class);
  private static final String MANIFEST_FILE = "_symlink_format_manifest/manifest";
  private final ExportStorageProvider exportStorageProvider;

  public ManifestFileManager(final ExportStorageProvider exportStorageProvider)
  {
    this.exportStorageProvider = exportStorageProvider;
  }

  public void createManifestFile(Set<String> exportedFiles)
  {
    try {
      final StorageConnector storageConnector = exportStorageProvider.get();
      log.info("Writing manifest file at [%s]", exportStorageProvider.getBasePath());

      if (storageConnector.pathExists(MANIFEST_FILE)) {
        throw DruidException.defensive("Found manifest file already present at path.");
      }

      try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(storageConnector.write(MANIFEST_FILE), StandardCharsets.UTF_8))) {
        for (String exportedFile : exportedFiles) {
          printWriter.println(exportStorageProvider.getFilePathForManifest(exportedFile));
        }
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
