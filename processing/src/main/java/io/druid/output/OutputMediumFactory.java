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

package io.druid.output;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, defaultImpl = TmpFileOutputMediumFactory.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "tmpFile", value = TmpFileOutputMediumFactory.class),
    @JsonSubTypes.Type(name = "offHeapMemory", value = OffHeapMemoryOutputMediumFactory.class),
})
public interface OutputMediumFactory
{
  static Set<OutputMediumFactory> builtInFactories()
  {
    return ImmutableSet.of(TmpFileOutputMediumFactory.instance(), OffHeapMemoryOutputMediumFactory.instance());
  }

  /**
   * Creates a new OutputMedium. If this type of OutputMedium needs to create some temprorary files, it creates
   * a *subdirectory* in the given outDir, stores the files there, and removes the files and the subdirectory when
   * closed.
   */
  OutputMedium makeOutputMedium(File outDir) throws IOException;
}
