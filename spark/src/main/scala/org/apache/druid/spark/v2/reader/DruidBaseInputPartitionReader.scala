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

package org.apache.druid.spark.v2.reader

import com.fasterxml.jackson.core.`type`.TypeReference
import org.apache.druid.java.util.common.FileUtils
import org.apache.druid.segment.{Segment, SegmentLazyLoadFailCallback}
import org.apache.druid.segment.loading.SegmentLoader
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.configuration.{Configuration, DruidConfigurationKeys, SerializableHadoopConfiguration}
import org.apache.druid.spark.mixins.Logging
import org.apache.druid.spark.registries.{ComplexTypeRegistry, SegmentReaderRegistry}
import org.apache.druid.spark.utils.{NullHandlingUtils, PartitionReaderSegmentLoader}
import org.apache.druid.spark.v2.INDEX_IO
import org.apache.druid.timeline.DataSegment
import org.apache.spark.broadcast.Broadcast

import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

class DruidBaseInputPartitionReader(
                                     segmentStr: String,
                                     columnTypes: Option[Set[String]],
                                     broadcastedHadoopConf: Broadcast[SerializableHadoopConfiguration],
                                     conf: Configuration,
                                     useSparkConfForDeepStorage: Boolean,
                                     useCompactSketches: Boolean,
                                     useDefaultNullHandling: Boolean
                               ) extends Logging {
  // Need to initialize Druid's internal null handling as well for filters etc.
  NullHandlingUtils.initializeDruidNullHandling(useDefaultNullHandling)

  if (columnTypes.isDefined) {
    // Callers will need to explicitly register any complex types not known to ComplexTypeRegistry by default
    columnTypes.get.foreach {
      ComplexTypeRegistry.registerByName(_, useCompactSketches)
    }
  } else {
    ComplexTypeRegistry.initializeDefaults()
  }
  ComplexTypeRegistry.registerSerdes()

  // If there are mixed deep storage types, callers will need to handle initialization themselves.
  if (!useSparkConfForDeepStorage && DruidBaseInputPartitionReader.initialized.compareAndSet(false, true)) {
    val deepStorageType = conf.get(DruidConfigurationKeys.deepStorageTypeDefaultKey)
    SegmentReaderRegistry.registerInitializerByType(deepStorageType)
    SegmentReaderRegistry.initialize(deepStorageType, conf.dive(deepStorageType))
  }

  private[reader] val dataSegment =
    MAPPER.readValue[DataSegment](segmentStr, new TypeReference[DataSegment] {})
  private[reader] lazy val hadoopConf = broadcastedHadoopConf.value.value
  private[reader] lazy val tmpDir: File = FileUtils.createTempDir
  private[reader] lazy val segmentLoader: SegmentLoader = new PartitionReaderSegmentLoader(tmpDir, hadoopConf, INDEX_IO)
  private[reader] lazy val segment: Segment =
    segmentLoader.getSegment(dataSegment, false, SegmentLazyLoadFailCallback.NOOP)
}

private[reader] object DruidBaseInputPartitionReader {
  private val initialized = new AtomicBoolean(false)
}
