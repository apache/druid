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

package org.apache.druid.storage.s3;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.IAE;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;

/**
 * Stores the configuration for options related to reading
 * input data from Amazon S3 into Druid
 */
public class S3InputDataConfig
{
  @VisibleForTesting
  static final int MAX_LISTING_LENGTH_MIN = 1;

  /**
   * AWS S3 only allows listing and deleting 1000 elements at a time:
   * https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjects.html#API_ListObjects_RequestSyntax
   * https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObjects.html#API_DeleteObjects_RequestSyntax
   */
  @VisibleForTesting
  static final int MAX_LISTING_LENGTH_MAX = 1000;

  /**
   * The maximum number of input files matching a given prefix to retrieve
   * or delete from Amazon S3 at a time.
   */
  @JsonProperty
  @Min(MAX_LISTING_LENGTH_MIN)
  @Max(MAX_LISTING_LENGTH_MAX)
  private int maxListingLength = MAX_LISTING_LENGTH_MAX;

  @VisibleForTesting
  public void setMaxListingLength(int maxListingLength)
  {
    if (maxListingLength < MAX_LISTING_LENGTH_MIN || maxListingLength > MAX_LISTING_LENGTH_MAX) {
      throw new IAE("valid values for maxListingLength are between [%d, %d]", MAX_LISTING_LENGTH_MIN,
                    MAX_LISTING_LENGTH_MAX
      );
    }
    this.maxListingLength = maxListingLength;
  }

  public int getMaxListingLength()
  {
    return maxListingLength;
  }
}
