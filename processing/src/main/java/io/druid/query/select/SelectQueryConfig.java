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

package io.druid.query.select;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SelectQueryConfig
{
  public static String ENABLE_FROM_NEXT_DEFAULT = "enableFromNextDefault";

  @JsonProperty
  private boolean enableFromNextDefault = true;

  @JsonCreator
  public SelectQueryConfig(
      @JsonProperty("enableFromNextDefault") Boolean enableFromNextDefault
  )
  {
    if (enableFromNextDefault != null) {
      this.enableFromNextDefault = enableFromNextDefault.booleanValue();
    }
  }

  public boolean getEnableFromNextDefault()
  {
    return enableFromNextDefault;
  }

  public void setEnableFromNextDefault(boolean enableFromNextDefault)
  {
    this.enableFromNextDefault = enableFromNextDefault;
  }
}
