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

package org.apache.druid.spark.configuration

import org.apache.spark.sql.sources.v2.DataSourceOptions

object DruidConfigurationKeys {
  // Shadowing DataSourceOptions.TABLE_KEY here so other classes won't need unnecessary imports
  val tableKey: String = DataSourceOptions.TABLE_KEY

  // Metadata Client Configs
  val metadataPrefix: String = "metadata"
  val metadataDbTypeKey: String = "dbType"
  val metadataHostKey: String = "host"
  val metadataPortKey: String = "port"
  val metadataConnectUriKey: String = "connectUri"
  val metadataUserKey: String = "user"
  val metadataPasswordKey: String = "password"
  val metadataDbcpPropertiesKey: String = "dbcpProperties"
  val metadataBaseNameKey: String = "baseName"
  private[spark] val metadataHostDefaultKey: (String, String) = (metadataHostKey, "localhost")
  private[spark] val metadataPortDefaultKey: (String, Int) = (metadataPortKey, 1527)
  private[spark] val metadataBaseNameDefaultKey: (String, String) = (metadataBaseNameKey, "druid")

  // Druid Client Configs
  val brokerPrefix: String = "broker"
  val brokerHostKey: String = "host"
  val brokerPortKey: String = "port"
  private[spark] val brokerHostDefaultKey: (String, String) = (brokerHostKey, "localhost")
  private[spark] val brokerPortDefaultKey: (String, Int) = (brokerPortKey, 8082)

  // Reader Configs
  val readerPrefix: String = "reader"
  val segmentsKey: String = "segments"
  val useCompactSketchesKey: String = "useCompactSketches" // Default: false
  val useDefaultValueForNull: String = "useDefaultValueForNull" // Default: true
  val vectorizeKey: String = "vectorize" // Default: false, experimental key!
  val batchSizeKey: String = "batchSize" // Default: 512
  private[spark] val useCompactSketchesDefaultKey: (String, Boolean) = (useCompactSketchesKey, false)
  private[spark] val useDefaultValueForNullDefaultKey: (String, Boolean) = (useDefaultValueForNull, true)
  private[spark] val vectorizeDefaultKey: (String, Boolean) = (vectorizeKey, false)
  private[spark] val batchSizeDefaultKey: (String, Int) = (batchSizeKey, 512)

  // Writer Configs
  val writerPrefix: String = "writer"
  val versionKey: String = "version"
  val dimensionsKey: String = "dimensions"
  val metricsKey: String = "metrics"
  val excludedDimensionsKey: String = "excludedDimensions"
  val segmentGranularityKey: String = "segmentGranularity"
  val queryGranularityKey: String = "queryGranularity"
  val partitionMapKey: String = "partitionMap"
  val deepStorageTypeKey: String = "deepStorageType"
  val timestampColumnKey: String = "timestampColumn"
  val timestampFormatKey: String = "timestampFormat"
  val shardSpecTypeKey: String = "shardSpecType"
  val rollUpSegmentsKey: String = "rollUpSegments"
  val rowsPerPersistKey: String = "rowsPerPersist"
  val rationalizeSegmentsKey: String = "rationalizeSegments"
  private[spark] val dimensionsDefaultKey: (String, String) = (dimensionsKey, "[]")
  private[spark] val metricsDefaultKey: (String, String) = (metricsKey, "[]")
  private[spark] val segmentGranularityDefaultKey: (String, String) = (segmentGranularityKey, "ALL")
  private[spark] val queryGranularityDefaultKey: (String, String) = (queryGranularityKey, "None")
  private[spark] val deepStorageTypeDefaultKey: (String, String) = (deepStorageTypeKey, "local")
  private[spark] val timeStampColumnDefaultKey: (String, String) = (timestampColumnKey, "ts")
  private[spark] val timestampFormatDefaultKey: (String, String) = (timestampFormatKey, "auto")
  private[spark] val shardSpecTypeDefaultKey: (String, String) = (shardSpecTypeKey, "numbered")
  private[spark] val rollUpSegmentsDefaultKey: (String, Boolean) = (rollUpSegmentsKey, true)
  private[spark] val rowsPerPersistDefaultKey: (String, Int) = (rowsPerPersistKey, 2000000)
  private[spark] val rationalizeSegmentsDefaultKey: (String, Boolean) = (rationalizeSegmentsKey, true)

  // IndexSpec Configs for Writer
  val indexSpecPrefix: String = "indexSpec"
  val bitmapTypeKey: String = "bitmap"
  val compressRunOnSerializationKey: String = "compressRunOnSerialization"
  val dimensionCompressionKey: String = "dimensionCompression"
  val metricCompressionKey: String = "metricCompression"
  val longEncodingKey: String = "longEncoding"
  private[spark] val bitmapTypeDefaultKey: (String, String) = (bitmapTypeKey, "roaring")
  private[spark] val compressRunOnSerializationDefaultKey: (String, Boolean) = (compressRunOnSerializationKey, true)

  // Local SegmentWriter Configs
  val localDeepStorageTypeKey: String = "local"
  val localStorageDirectoryKey: String = "storageDirectory"

  // HDFS SegmentWriter Configs
  val hdfsDeepStorageTypeKey: String = "hdfs"
  val hdfsHadoopConfKey: String = "hadoopConf" // Base64-encoded serialized Configuration

  // S3 SegmentWriter Configs
  val s3DeepStorageTypeKey: String = "s3"
  val s3UseS3ASchemaKey: String = "useS3aSchema"
  val s3StorageConfigKey: String = "s3StorageConfig"
  val s3ServerSideEncryptionPrefix: String = "sse"
  val s3ServerSideEncryptionTypeKey: String = "type"
  val s3ServerSideEncryptionKmsKeyIdKey: String = "keyId"
  val s3ServerSideEncryptionCustomKeyKey: String = "base64EncodedKey"

  // GCS SegmentWriter Configs
  val googleDeepStorageTypeKey: String = "google"

  // Azure SegmentWriter Configs
  val azureDeepStorageKey: String = "azure"
  val azureConnectionStringKey: String = "connectionString"
  val azurePrimaryStorageUriKey: String = "primaryUri"
  val azureSecondaryStorageUriKey: String = "secondaryUri"
  val azurePrefixesKey: String = "prefixes" // Note that there is both a prefix and a prefixes key!
  val azureMaxListingLengthKey: String = "maxListingLength"
  val azureCloudBlobIterableFactoryConfigKey: String = "azureCloudBlobIterableFactoryConfig"
  private[spark] val azureMaxListingLengthDefaultKey: (String, Int) = (azureMaxListingLengthKey, 1024)
}
