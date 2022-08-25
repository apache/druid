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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;

import java.util.List;
import java.util.Map;

/**
 * Slicer that handles multiple types of specs.
 */
public class MapInputSpecSlicer implements InputSpecSlicer
{
  private final Map<Class<? extends InputSpec>, InputSpecSlicer> splitterMap;

  public MapInputSpecSlicer(final Map<Class<? extends InputSpec>, InputSpecSlicer> splitterMap)
  {
    this.splitterMap = ImmutableMap.copyOf(splitterMap);
  }

  @Override
  public boolean canSliceDynamic(InputSpec inputSpec)
  {
    return getSlicer(inputSpec.getClass()).canSliceDynamic(inputSpec);
  }

  @Override
  public List<InputSlice> sliceStatic(InputSpec inputSpec, int maxNumSlices)
  {
    return getSlicer(inputSpec.getClass()).sliceStatic(inputSpec, maxNumSlices);
  }

  @Override
  public List<InputSlice> sliceDynamic(
      InputSpec inputSpec,
      int maxNumSlices,
      int maxFilesPerSlice,
      long maxBytesPerSlice
  )
  {
    return getSlicer(inputSpec.getClass()).sliceDynamic(inputSpec, maxNumSlices, maxFilesPerSlice, maxBytesPerSlice);
  }

  private InputSpecSlicer getSlicer(final Class<? extends InputSpec> clazz)
  {
    final InputSpecSlicer slicer = splitterMap.get(clazz);

    if (slicer == null) {
      throw new ISE("Cannot handle inputSpec of class [%s]", clazz.getName());
    }

    return slicer;
  }
}
