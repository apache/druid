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

package org.apache.druid.js;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.guice.annotations.PublicApi;

/**
 * Should be used by extension filters, aggregators, etc, that use JavaScript to determine if JavaScript is enabled
 * or not.
 */
@PublicApi
public class JavaScriptConfig
{
  public static final int DEFAULT_OPTIMIZATION_LEVEL = 9;

  private static final JavaScriptConfig ENABLED_INSTANCE = new JavaScriptConfig(true);

  @JsonProperty
  private final boolean enabled;

  @JsonCreator
  public JavaScriptConfig(
      @JsonProperty("enabled") boolean enabled
  )
  {
    this.enabled = enabled;
  }

  public boolean isEnabled()
  {
    return enabled;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    JavaScriptConfig that = (JavaScriptConfig) o;

    return enabled == that.enabled;

  }

  @Override
  public int hashCode()
  {
    return (enabled ? 1 : 0);
  }

  @Override
  public String toString()
  {
    return "JavaScriptConfig{" +
           "enabled=" + enabled +
           '}';
  }

  public static JavaScriptConfig getEnabledInstance()
  {
    return ENABLED_INSTANCE;
  }
}
