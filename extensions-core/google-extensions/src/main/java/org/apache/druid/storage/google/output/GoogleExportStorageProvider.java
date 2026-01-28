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

package org.apache.druid.storage.google.output;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import org.apache.druid.data.input.google.GoogleCloudStorageInputSource;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleStorageDruidModule;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.net.URI;
import java.util.List;

@JsonTypeName(GoogleExportStorageProvider.TYPE_NAME)
public class GoogleExportStorageProvider implements ExportStorageProvider
{
  public static final String TYPE_NAME = GoogleCloudStorageInputSource.TYPE_KEY;
  private static final String DELIM = "/";
  private static final Joiner JOINER = Joiner.on(DELIM).skipNulls();
  @JsonProperty
  private final String bucket;
  @JsonProperty
  private final String prefix;

  @JacksonInject
  GoogleExportConfig googleExportConfig;
  @JacksonInject
  GoogleStorage googleStorage;
  @JacksonInject
  GoogleInputDataConfig googleInputDataConfig;

  @JsonCreator
  public GoogleExportStorageProvider(
      @JsonProperty(value = "bucket", required = true) String bucket,
      @JsonProperty(value = "prefix", required = true) String prefix
  )
  {
    this.bucket = bucket;
    this.prefix = prefix;
  }

  @Override
  public StorageConnector createStorageConnector(File taskTempDir)
  {
    final String exportConfigTempDir = googleExportConfig.getTempLocalDir();
    final File tempDirFile = exportConfigTempDir != null ? new File(exportConfigTempDir) : taskTempDir;
    if (tempDirFile == null) {
      throw DruidException.defensive("Couldn't find temporary directory for export.");
    }
    final List<String> allowedExportPaths = googleExportConfig.getAllowedExportPaths();
    if (allowedExportPaths == null) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.NOT_FOUND)
                          .build(
                              "The runtime property `druid.export.storage.google.allowedExportPaths` must be configured for GCS export.");
    }
    validatePrefix(allowedExportPaths, bucket, prefix);
    final GoogleOutputConfig googleOutputConfig = new GoogleOutputConfig(
        bucket,
        prefix,
        tempDirFile,
        googleExportConfig.getChunkSize(),
        googleExportConfig.getMaxRetry()
    );
    return new GoogleStorageConnector(googleOutputConfig, googleStorage, googleInputDataConfig);
  }

  @VisibleForTesting
  static void validatePrefix(@NotNull final List<String> allowedExportPaths, final String bucket, final String prefix)
  {
    final URI providedUri = new CloudObjectLocation(bucket, prefix).toUri(GoogleStorageDruidModule.SCHEME_GS);
    for (final String path : allowedExportPaths) {
      final URI allowedUri = URI.create(path);
      if (validateUri(allowedUri, providedUri)) {
        return;
      }
    }
    throw DruidException.forPersona(DruidException.Persona.USER)
                        .ofCategory(DruidException.Category.INVALID_INPUT)
                        .build("None of the allowed prefixes matched the input path [%s]. "
                               + "Please reach out to the cluster admin for the whitelisted paths for export. "
                               + "The paths are controlled via the property `druid.export.storage.google.allowedExportPaths`.",
                               providedUri);
  }

  private static boolean validateUri(final URI allowedUri, final URI providedUri)
  {
    if (!allowedUri.getHost().equals(providedUri.getHost())) {
      return false;
    }
    final String allowedPath = StringUtils.maybeAppendTrailingSlash(allowedUri.getPath());
    final String providedPath = StringUtils.maybeAppendTrailingSlash(providedUri.getPath());
    return providedPath.startsWith(allowedPath);
  }

  @JsonProperty("bucket")
  public String getBucket()
  {
    return bucket;
  }

  @JsonProperty("prefix")
  public String getPrefix()
  {
    return prefix;
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
    return new CloudObjectLocation(bucket, prefix).toUri(GoogleStorageDruidModule.SCHEME_GS).toString();
  }

  @Override
  public String getFilePathForManifest(String fileName)
  {
    return new CloudObjectLocation(bucket, JOINER.join(prefix, fileName)).toUri(GoogleStorageDruidModule.SCHEME_GS).toString();
  }
}
