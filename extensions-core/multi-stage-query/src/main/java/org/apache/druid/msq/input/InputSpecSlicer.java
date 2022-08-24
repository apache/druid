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

package org.apache.druid.msq.input;

import java.util.List;

/**
 * Slices {@link InputSpec} into {@link InputSlice} on the controller.
 */
public interface InputSpecSlicer
{
  boolean canSliceDynamic(InputSpec inputSpec);

  /**
   * Slice a spec into a given maximum number of slices. The returned list may contain fewer slices, but cannot
   * contain more.
   *
   * This method creates as many slices as possible while staying at or under maxNumSlices. For example, if a spec
   * contains 8 files, and maxNumSlices is 10, then 8 slices will be created.
   */
  List<InputSlice> sliceStatic(InputSpec inputSpec, int maxNumSlices);

  /**
   * Slice a spec based on a particular maximum number of files and bytes per slice.
   *
   * This method creates as few slices as possible, while keeping each slice under the provided limits.
   *
   * If there is a conflict between maxNumSlices and maxFilesPerSlice or maxBytesPerSlice, then maxNumSlices wins.
   * This means that for small values of maxNumSlices, slices may have more than maxFilesPerSlice files, or more
   * than maxBytesPerSlice bytes.
   */
  List<InputSlice> sliceDynamic(InputSpec inputSpec, int maxNumSlices, int maxFilesPerSlice, long maxBytesPerSlice);
}
