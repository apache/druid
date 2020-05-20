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
import com.google.common.annotations.VisibleForTesting;

import javax.validation.constraints.NotNull;

public class OssTaskLogsConfig
{
  @JsonProperty
  @NotNull
  private String bucket = null;

  @JsonProperty
  @NotNull
  private String prefix = null;

  @JsonProperty
  private boolean disableAcl = false;

  @VisibleForTesting
  void setDisableAcl(boolean disableAcl)
  {
    this.disableAcl = disableAcl;
  }

  public String getBucket()
  {
    return bucket;
  }

  @VisibleForTesting
  void setBucket(String bucket)
  {
    this.bucket = bucket;
  }

  public String getPrefix()
  {
    return prefix;
  }

  @VisibleForTesting
  void setPrefix(String prefix)
  {
    this.prefix = prefix;
  }

  public boolean getDisableAcl()
  {
    return disableAcl;
  }

}
