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

package org.apache.druid.spark.utils

object DruidDataSourceOptionKeys {
  // TODO: Replace all of these unique keys with namespaced keys

  // Metadata Client Configs
  val metadataDbTypeKey: String = "metadataDbType"
  val metadataHostKey: String = "metadataHost" // Default: localhost
  val metadataPortKey: String = "metadataPort"
  val metadataConnectUriKey: String = "metadataConnectUri"
  val metadataUserKey: String = "metadataUser"
  val metadataPasswordKey: String = "metadataPassword"
  val metadataDbcpPropertiesKey: String = "metadataDbcpProperties"
  val metadataBaseNameKey: String = "metadataBaseName" // Default: druid

  // Druid Client Configs
  val brokerHostKey: String = "brokerHost" // Default: localhost
  val brokerPortKey: String = "brokerPort" // Default: 8082

  // Reader Configs
  val segmentsKey: String = "segments"
  val useCompactSketchesKey: String = "useCompactSketches" // Default: false

  // Writer Configs
  val versionKey: String = "version"
  val dimensionsKey: String = "dimensions"
  val metricsKey: String = "metrics"
  val excludedDimensionsKey: String = "excludedDimensions"
  val segmentGranularity: String = "segmentGranularity" // Default: All
  val queryGranularity: String = "queryGranularity" // Default: None
  val partitionMapKey: String = "partitionMap"
  val deepStorageTypeKey: String = "deepStorageType" // Default: local
  val timestampColumnKey: String = "timestampColumn" // Default: ts
  val timestampFormatKey: String = "timestampFormat" // Default: auto
  val shardSpecTypeKey: String = "shardSpecType" // Default: numbered
  val rollUpSegmentsKey: String = "rollUpSegments" // Default: true
  val rowsPerPersistKey: String = "rowsPerPersist" // Default: 2000000
  val rationalizeSegmentsKey: String = "rationalizeSegments" // Default: true

  // Shared SegmentWriter Configs
  val storageDirectoryKey: String = "storageDirectory"
  val maxListingLengthKey: String = "maxListingLength" // Default: 1024
  val connectionStringKey: String = "connectionString"
  val bucketKey: String = "bucket"
  val prefixKey: String = "prefix"

  // HDFS SegmentWriter Configs
  val hdfsDeepStorageTypeKey: String = "hdfs"
  val hdfsHadoopConfKey: String = "hadoopConf" // Base64-encoded serialized Configuration

  // S3 SegmentWriter Configs
  val s3AwsCredentialsConfigKey: String = "awsCredentialsConfig"
  val s3AccessKeyKey: String = "s3AccessKey"
  val s3SecretKeyKey: String = "s3SecretKey"
  val s3FileSessionCredentialsKey: String = "s3FileSessionCredentialsKey"
  val s3AwsProxyConfigKey: String = "awsProxyConfig"
  val s3ProxyHostKey: String = "s3ProxyHost"
  val s3ProxyPortKey: String = "s3ProxyPort" // Default -1
  val s3ProxyUserKey: String = "s3ProxyUser"
  val s3ProxyPasswordKey: String = "s3ProxyPassword"
  val s3AwsEndpointConfigKey: String = "awsEndpointConfig"
  val s3EndpointConfigUrlKey: String = "s3EndpointConfigUrl"
  val s3EndpointConfigSigningRegionKey: String = "s3EndpointConfigSigningRegion"
  val s3AwsClientConfigKey: String = "awsClientConfig"
  val s3ClientConfigProtocolKey: String = "s3ClientConfigProtocol" // Default: https
  val s3ClientConfigDisableChunkedEncodingKey: String = "s3DisableChunkedEncoding" // Default: false
  val s3ClientConfigEnablePathStyleAccessKey: String = "s3EnablePathStyleAccess" // Default: false
  val s3ClientConfigForceGlobalBucketAccessEnabledKey: String = "s3ForceGlobalBucketAccessEnabled" // Default: false
  val s3StorageConfigKey: String = "s3StorageConfig"
  val s3ServerSideEncryptionTypeKey: String = "s3ServerSideEncryptionType"
  val s3ServerSideEncryptionKmsKeyIdKey: String = "s3ServerSideEncryptionKmsKeyId"
  val s3ServerSideEncryptionCustomKeyKey: String = "s3ServerSideEncryptionCustomKey"
  val s3BaseKeyKey: String = "baseKey"
  val s3DisableACLKey: String = "disableAcl" // Default: false
  val s3UseS3ASchemaKey: String = "useS3Schema" // Default: true (NOTE THIS DIFFERS FROM S3DataSegmentPusherConfig!)

  // GCS SegmentWriter Configs
  val googleDeepStorageTypeKey: String = "google"

  // Azure SegmentWriter Configs
  val azureDeepStorageKey: String = "azure"
  val azureContainerKey: String = "container"
  val azureProtocolKey: String = "protocol" // Default: https
  val azureMaxTriesKey: String = "maxTries" // Default: 3
  val azureAccountKey: String = "account"
  val azureKeyKey: String = "key"
  val azurePrimaryStorageUriKey: String = "primaryUri"
  val azureSecondaryStorageUriKey: String = "secondaryUri"
  val azurePrefixesKey: String = "prefixes" // Note that there is both a prefix and a prefixes key!
  val azureAccountConfigKey: String = "azureAccountConfig"
  val azureCloudBlobIterableFactoryConfigKey: String = "azureCloudBlobIterableFactoryConfig"
}
