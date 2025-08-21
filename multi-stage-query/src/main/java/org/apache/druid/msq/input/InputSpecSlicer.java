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
 * Slices {@link InputSpec} into {@link InputSlice} on the controller. Each slice is assigned to a single worker, and
 * the slice number equals the worker number.
 */
public interface InputSpecSlicer
{
  /**
   * Whether {@link #sliceDynamic(InputSpec, int, int, long)} is usable for a given {@link InputSpec}.
   */
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
   *
   * The design of this method assumes that the ideal number of {@link InputSlice} can be determined by looking at
   * just one {@link InputSpec} at a time. This makes sense today, since there are no situations where a
   * {@link org.apache.druid.msq.kernel.StageDefinition} would be created with two {@link InputSpec} other than
   * {@link org.apache.druid.msq.input.stage.StageInputSpec} (which is not dynamically splittable, so would not
   * use this method anyway). If this changes in the future, we'll want to revisit the design of this method.
   *
   * @throws UnsupportedOperationException if {@link #canSliceDynamic(InputSpec)} returns false
   */
  List<InputSlice> sliceDynamic(InputSpec inputSpec, int maxNumSlices, int maxFilesPerSlice, long maxBytesPerSlice);
}
