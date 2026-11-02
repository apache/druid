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

import javax.annotation.Nullable;

/**
 * Role assumed before the {@code assumeRoleArn} named by an ingestion spec or export destination
 */
public class S3IntermediateRoleConfig
{
  @Nullable
  @JsonProperty
  private String intermediateAssumeRoleArn;

  public S3IntermediateRoleConfig()
  {
  }

  @VisibleForTesting
  S3IntermediateRoleConfig(@Nullable String intermediateAssumeRoleArn)
  {
    this.intermediateAssumeRoleArn = intermediateAssumeRoleArn;
  }

  @Nullable
  public String getIntermediateAssumeRoleArn()
  {
    return intermediateAssumeRoleArn;
  }

  @Override
  public String toString()
  {
    return "S3IntermediateRoleConfig{" +
           "intermediateAssumeRoleArn='" + intermediateAssumeRoleArn + '\'' +
           '}';
  }
}
