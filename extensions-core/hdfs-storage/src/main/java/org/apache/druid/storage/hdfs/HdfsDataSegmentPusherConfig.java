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

package org.apache.druid.storage.hdfs;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.utils.CompressionUtils;

/**
 */
public class HdfsDataSegmentPusherConfig
{
  @JsonProperty
  private String storageDirectory = "";

  @JsonProperty
  private CompressionUtils.Format compressionFormat = CompressionUtils.Format.ZIP;

  public void setStorageDirectory(String storageDirectory)
  {
    this.storageDirectory = storageDirectory;
  }

  public String getStorageDirectory()
  {
    return storageDirectory;
  }

  public void setCompressionFormat(CompressionUtils.Format compressionFormat)
  {
    this.compressionFormat = compressionFormat;
  }

  public CompressionUtils.Format getCompressionFormat()
  {
    return compressionFormat;
  }
}
