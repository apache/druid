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

package org.apache.druid.storage.s3.output;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.s3.S3InputSource;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.storage.ExportStorageProvider;
import org.apache.druid.storage.StorageConnector;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.apache.druid.storage.s3.ServerSideEncryptingAmazonS3;

import javax.validation.constraints.NotNull;
import java.io.File;
import java.net.URI;
import java.util.List;

@JsonTypeName(S3ExportStorageProvider.TYPE_NAME)
public class S3ExportStorageProvider implements ExportStorageProvider
{
  public static final String TYPE_NAME = S3InputSource.TYPE_KEY;
  @JsonProperty
  private final String bucket;
  @JsonProperty
  private final String prefix;

  @JacksonInject
  S3ExportConfig s3ExportConfig;
  @JacksonInject
  ServerSideEncryptingAmazonS3 s3;

  @JsonCreator
  public S3ExportStorageProvider(
      @JsonProperty(value = "bucket", required = true) String bucket,
      @JsonProperty(value = "prefix", required = true) String prefix
  )
  {
    this.bucket = bucket;
    this.prefix = prefix;
  }

  @Override
  public StorageConnector get()
  {
    final String tempDir = s3ExportConfig.getTempLocalDir();
    if (tempDir == null) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.NOT_FOUND)
                          .build("The runtime property `druid.export.storage.s3.tempLocalDir` must be configured for S3 export.");
    }
    final List<String> allowedExportPaths = s3ExportConfig.getAllowedExportPaths();
    if (allowedExportPaths == null) {
      throw DruidException.forPersona(DruidException.Persona.OPERATOR)
                          .ofCategory(DruidException.Category.NOT_FOUND)
                          .build(
                              "The runtime property `druid.export.storage.s3.allowedExportPaths` must be configured for S3 export.");
    }
    validateS3Prefix(allowedExportPaths, bucket, prefix);
    final S3OutputConfig s3OutputConfig = new S3OutputConfig(
        bucket,
        prefix,
        new File(tempDir),
        s3ExportConfig.getChunkSize(),
        s3ExportConfig.getMaxRetry()
    );
    return new S3StorageConnector(s3OutputConfig, s3);
  }

  @VisibleForTesting
  static void validateS3Prefix(@NotNull final List<String> allowedExportPaths, final String bucket, final String prefix)
  {
    final URI providedUri = new CloudObjectLocation(bucket, prefix).toUri(S3StorageDruidModule.SCHEME);
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
                               + "The paths are controlled via the property `druid.export.storage.s3.allowedExportPaths`.",
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
    return new CloudObjectLocation(bucket, prefix).toUri(S3StorageDruidModule.SCHEME).toString();
  }
}
