/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.s3;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.Min;

/**
 */
public class S3DataSegmentPusherConfig
{
  @JsonProperty
  private String bucket = "";

  @JsonProperty
  private String baseKey = "";

  @JsonProperty
  private boolean disableAcl = false;

  @JsonProperty
  @Min(0)
  private int maxListingLength = 1000;
  // use s3n by default for backward compatibility
  @JsonProperty
  private boolean useS3aSchema = false;

  public void setBucket(String bucket)
  {
    this.bucket = bucket;
  }

  public void setBaseKey(String baseKey)
  {
    this.baseKey = baseKey;
  }

  public void setDisableAcl(boolean disableAcl)
  {
    this.disableAcl = disableAcl;
  }

  public void setMaxListingLength(int maxListingLength)
  {
    this.maxListingLength = maxListingLength;
  }

  public boolean isUseS3aSchema()
  {
    return useS3aSchema;
  }

  public void setUseS3aSchema(boolean useS3aSchema)
  {
    this.useS3aSchema = useS3aSchema;
  }

  public String getBucket()
  {
    return bucket;
  }

  public String getBaseKey()
  {
    return baseKey;
  }

  public boolean getDisableAcl()
  {
    return disableAcl;
  }

  public int getMaxListingLength()
  {
    return maxListingLength;
  }
}
