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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.server.security.ResourceAction;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface SamplerSpec
{
  SamplerResponse sample();

  /**
   * Returns the type of this sampler type.
   *
   * @return sampler spec type label
   */

  @Nullable
  default String getType()
  {
    return null;
  }

  /**
   * @return The types of {@link org.apache.druid.data.input.InputSource} that the sampler spec uses.
   * Empty set is returned if the sampler spec does not use any. Users can be given permission to access
   * particular types of input sources but not others, using the
   * {@link org.apache.druid.server.security.AuthConfig#enableInputSourceSecurity} config.
   */
  @JsonIgnore
  @Nonnull
  default Set<ResourceAction> getInputSourceResources() throws UOE
  {
    throw new UOE(StringUtils.format(
        "SamplerSpec type [%s], does not support input source based security",
        getType()
    ));
  }
}
