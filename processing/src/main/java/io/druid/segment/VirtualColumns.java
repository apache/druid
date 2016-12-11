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

package io.druid.segment;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 */
public class VirtualColumns
{
  public static final VirtualColumns EMPTY = new VirtualColumns(
      ImmutableMap.<String, VirtualColumn>of(), ImmutableMap.<String, VirtualColumn>of()
  );

  public static VirtualColumns valueOf(List<VirtualColumn> virtualColumns) {
    if (virtualColumns == null || virtualColumns.isEmpty()) {
      return EMPTY;
    }
    Map<String, VirtualColumn> withDotSupport = Maps.newHashMap();
    Map<String, VirtualColumn> withoutDotSupport = Maps.newHashMap();
    for (VirtualColumn vc : virtualColumns) {
      if (vc.usesDotNotation()) {
        withDotSupport.put(vc.getOutputName(), vc);
      } else {
        withoutDotSupport.put(vc.getOutputName(), vc);
      }
    }
    return new VirtualColumns(withDotSupport, withoutDotSupport);
  }

  public VirtualColumns(Map<String, VirtualColumn> withDotSupport, Map<String, VirtualColumn> withoutDotSupport)
  {
    this.withDotSupport = withDotSupport;
    this.withoutDotSupport = withoutDotSupport;
  }

  private final Map<String, VirtualColumn> withDotSupport;
  private final Map<String, VirtualColumn> withoutDotSupport;

  public VirtualColumn getVirtualColumn(String dimension)
  {
    VirtualColumn vc = withoutDotSupport.get(dimension);
    if (vc != null) {
      return vc;
    }
    for (int index = dimension.indexOf('.'); index >= 0; index = dimension.indexOf('.', index + 1)) {
      vc = withDotSupport.get(dimension.substring(0, index));
      if (vc != null) {
        return vc;
      }
    }
    return withDotSupport.get(dimension);
  }

  public boolean isEmpty()
  {
    return withDotSupport.isEmpty() && withoutDotSupport.isEmpty();
  }
}
