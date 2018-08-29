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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import org.apache.druid.guice.annotations.ExtensionPoint;

import java.io.File;

/**
 * A means of pulling segment files into a destination directory
 */
@ExtensionPoint
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface LoadSpec
{
  /**
   * Method should put the segment files in the directory passed
   * @param destDir The destination directory
   * @return The byte count of data put in the destination directory
   */
  LoadSpecResult loadSegment(File destDir) throws SegmentLoadingException;

  // Hold interesting data about the results of the segment load
  class LoadSpecResult
  {
    private final long size;

    public LoadSpecResult(long size)
    {
      this.size = size;
    }

    public long getSize()
    {
      return this.size;
    }
  }
}
