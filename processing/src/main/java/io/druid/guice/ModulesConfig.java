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

package io.druid.guice;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.List;

public class ModulesConfig
{
  /**
   * Canonical class names of modules, which should not be loaded despite they are founded in extensions from {@link
   * ExtensionsConfig#loadList} or the standard list of modules loaded by some node type, e. g. {@code
   * CliPeon}.
   */
  @JsonProperty
  private List<String> excludeList = Collections.emptyList();

  public List<String> getExcludeList()
  {
    return excludeList;
  }

  @Override
  public String toString()
  {
    return "ModulesConfig{" +
           "excludeList=" + excludeList +
           '}';
  }
}
