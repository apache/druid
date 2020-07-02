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

package org.apache.druid.storage.aliyun;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * Stores the configuration for options related to reading
 * input data from aliyun OSS into Druid
 */
public class OssInputDataConfig
{
  /**
   * The maximum number of input files matching a given prefix to retrieve
   * from aliyun OSS at a time.
   * valid range is [1,1000]
   */
  @JsonProperty
  @Min(1)
  @Max(OssUtils.MAX_LISTING_LENGTH)
  private int maxListingLength = OssUtils.MAX_LISTING_LENGTH;

  public void setMaxListingLength(int maxListingLength)
  {
    this.maxListingLength = maxListingLength;
  }

  public int getMaxListingLength()
  {
    return maxListingLength;
  }
}
