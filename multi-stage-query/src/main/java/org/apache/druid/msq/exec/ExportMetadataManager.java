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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.storage.StorageConnector;

import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Manages writing of metadata files during export queries.
 */
public class ExportMetadataManager
{
  public static final String SYMLINK_DIR = "_symlink_format_manifest";
  public static final String MANIFEST_FILE = SYMLINK_DIR + "/manifest";
  public static final String META_FILE = SYMLINK_DIR + "/druid_export_meta";
  public static final int MANIFEST_FILE_VERSION = 1;
  private static final Logger log = new Logger(ExportMetadataManager.class);
  private final ExportStorageProvider exportStorageProvider;
  private final File tmpDir;

  public ExportMetadataManager(final ExportStorageProvider exportStorageProvider, final File tmpDir)
  {
    this.exportStorageProvider = exportStorageProvider;
    this.tmpDir = tmpDir;
  }

  public void writeMetadata(List<String> exportedFiles) throws IOException
  {
    final StorageConnector storageConnector = exportStorageProvider.createStorageConnector(tmpDir);
    log.info("Writing manifest file at location [%s]", exportStorageProvider.getBasePath());

    if (storageConnector.pathExists(MANIFEST_FILE) || storageConnector.pathExists(META_FILE)) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.RUNTIME_FAILURE)
                          .build("Found existing manifest file already present at path.");
    }

    createManifestFile(storageConnector, exportedFiles);
    createDruidMetadataFile(storageConnector);
  }

  /**
   * Creates a manifest file containing the list of files created by the export query. The manifest file consists of a
   * new line separated list. Each line contains the absolute path to a file created by the export.
   */
  public void createManifestFile(StorageConnector storageConnector, List<String> exportedFiles) throws IOException
  {
    try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(storageConnector.write(MANIFEST_FILE), StandardCharsets.UTF_8))) {
      for (String exportedFile : exportedFiles) {
        printWriter.println(exportStorageProvider.getFilePathForManifest(exportedFile));
      }
    }
  }

  /**
   * Creates a druid metadata file at the export location. This file contains extra information about the export, which
   * cannot be stored in the manifest directly, so that it can follow the symlink format.
   * <br>
   * Currently, this only contains the manifest file version.
   */
  private void createDruidMetadataFile(StorageConnector storageConnector) throws IOException
  {
    // Write the export manifest metadata information.
    // This includes only the version number currently.
    try (PrintWriter printWriter = new PrintWriter(new OutputStreamWriter(storageConnector.write(META_FILE), StandardCharsets.UTF_8))) {
      printWriter.println(StringUtils.format("version: %s", MANIFEST_FILE_VERSION));
    }
  }
}
