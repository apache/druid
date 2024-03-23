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

package org.apache.druid.storage.local;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.storage.StorageConfig;
import org.apache.druid.storage.StorageConnector;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

@JsonTypeName(LocalFileExportStorageProvider.TYPE_NAME)
public class LocalFileExportStorageProvider implements ExportStorageProvider
{
  public static final String TYPE_NAME = LocalInputSource.TYPE_KEY;

  @JacksonInject
  StorageConfig storageConfig;

  @JsonProperty
  private final String exportPath;

  @JsonCreator
  public LocalFileExportStorageProvider(@JsonProperty(value = "exportPath", required = true) String exportPath)
  {
    this.exportPath = exportPath;
  }

  @Override
  public StorageConnector get()
  {
    final File exportDestination = validateAndGetPath(storageConfig.getBaseDir(), exportPath);
    try {
      return new LocalFileStorageConnector(exportDestination);
    }
    catch (IOException e) {
      throw new IAE(
          e,
          "Unable to create storage connector [%s] for base path [%s]",
          LocalFileStorageConnector.class.getSimpleName(),
          exportDestination.toPath()
      );
    }
  }

  @Override
  @JsonIgnore
  public String getResourceType()
  {
    return TYPE_NAME;
  }

  @Override
  @JsonIgnore
  public String getBasePath()
  {
    return exportPath;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    LocalFileExportStorageProvider that = (LocalFileExportStorageProvider) o;
    return Objects.equals(exportPath, that.exportPath);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(exportPath);
  }

  @Override
  public String toString()
  {
    return "LocalFileExportStorageProvider{" +
           "exportPath=" + exportPath +
           '}';
  }

  public static File validateAndGetPath(String basePath, String customPath)
  {
    if (basePath == null) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.NOT_FOUND)
                          .build(
                              "The runtime property `druid.export.storage.baseDir` must be configured for local export.");
    }
    final File baseDir = new File(basePath);
    if (!baseDir.isAbsolute()) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build(
                              "The runtime property `druid.export.storage.baseDir` must be an absolute path.");
    }
    final File exportFile = new File(customPath);
    if (!exportFile.toPath().normalize().startsWith(baseDir.toPath())) {
      throw DruidException.forPersona(DruidException.Persona.USER)
                          .ofCategory(DruidException.Category.INVALID_INPUT)
                          .build("The provided destination [%s] must be within the path configured by runtime property `druid.export.storage.baseDir` "
                                 + "Please reach out to the cluster admin for the allowed path. ", customPath);
    }
    return exportFile;
  }
}
