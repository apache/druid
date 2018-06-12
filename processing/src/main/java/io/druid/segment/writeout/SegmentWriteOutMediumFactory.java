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

package io.druid.segment.writeout;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.collect.ImmutableSet;

import java.io.File;
import java.io.IOException;
import java.util.Set;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, defaultImpl = TmpFileSegmentWriteOutMediumFactory.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "tmpFile", value = TmpFileSegmentWriteOutMediumFactory.class),
    @JsonSubTypes.Type(name = "offHeapMemory", value = OffHeapMemorySegmentWriteOutMediumFactory.class),
})
public interface SegmentWriteOutMediumFactory
{
  static Set<SegmentWriteOutMediumFactory> builtInFactories()
  {
    return ImmutableSet.<SegmentWriteOutMediumFactory>of(
        TmpFileSegmentWriteOutMediumFactory.instance(),
        OffHeapMemorySegmentWriteOutMediumFactory.instance()
    );
  }

  /**
   * Creates a new SegmentWriteOutMedium. If this type of SegmentWriteOutMedium needs to create some temprorary files,
   * it creates a *subdirectory* in the given outDir, stores the files there, and removes the files and the subdirectory
   * when closed.
   */
  SegmentWriteOutMedium makeSegmentWriteOutMedium(File outDir) throws IOException;
}
