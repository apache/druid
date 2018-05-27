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
package io.druid.common.aws;

import com.amazonaws.services.s3.S3ClientOptions;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AWSClientConfig
{
  @JsonProperty
  private boolean disableChunkedEncoding = S3ClientOptions.DEFAULT_CHUNKED_ENCODING_DISABLED;

  @JsonProperty
  private boolean enablePathStyleAccess = S3ClientOptions.DEFAULT_PATH_STYLE_ACCESS;

  @JsonProperty
  protected boolean forceGlobalBucketAccessEnabled = S3ClientOptions.DEFAULT_FORCE_GLOBAL_BUCKET_ACCESS_ENABLED;

  public boolean isDisableChunkedEncoding()
  {
    return disableChunkedEncoding;
  }

  public boolean isEnablePathStyleAccess()
  {
    return enablePathStyleAccess;
  }

  public boolean isForceGlobalBucketAccessEnabled()
  {
    return forceGlobalBucketAccessEnabled;
  }
}
