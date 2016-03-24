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

package io.druid.segment.loading;

import io.druid.timeline.DataSegment;

import java.util.Set;

/**
 * A DataSegmentFinder is responsible for finding Druid segments underneath a specified directory and optionally updates
 * all descriptor.json files on deep storage with correct loadSpec.
 */
public interface DataSegmentFinder
{
  /**
   * This method should first recursively look for descriptor.json underneath workingDirPath and then verify that
   * index.zip exists in the same folder. If not, it should throw SegmentLoadingException to let the caller know that
   * descriptor.json exists while index.zip doesn't. If a segment is found and updateDescriptor is set, then this method
   * should update the loadSpec in descriptor.json to reflect the location from where it was found. After the search,
   * this method should return the set of segments that were found.
   *
   * @param workingDirPath   the String representation of the working directory path
   * @param updateDescriptor if true, update loadSpec in descriptor.json if loadSpec's location is different from where
   *                         desciptor.json was found
   *
   * @return a set of segments that were found underneath workingDirPath
   */
  Set<DataSegment> findSegments(String workingDirPath, boolean updateDescriptor) throws SegmentLoadingException;
}
