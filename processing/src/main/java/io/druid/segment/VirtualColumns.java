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
      ImmutableMap.<String, VirtualColumn>of()
  );

  public static VirtualColumns valueOf(List<VirtualColumn> virtualColumns) {
    if (virtualColumns == null || virtualColumns.isEmpty()) {
      return EMPTY;
    }
    Map<String, VirtualColumn> map = Maps.newHashMap();
    for (VirtualColumn vc : virtualColumns) {
      map.put(vc.getOutputName(), vc);
    }
    return new VirtualColumns(map);
  }

  private final Map<String, VirtualColumn> virtualColumns;

  public VirtualColumns(Map<String, VirtualColumn> virtualColumns)
  {
    this.virtualColumns = virtualColumns;
  }

  public VirtualColumn getVirtualColumn(String dimension)
  {
    int index = dimension.indexOf('.');
    return virtualColumns.get(index < 0 ? dimension : dimension.substring(0, index));
  }

  public boolean isEmpty()
  {
    return virtualColumns.isEmpty();
  }
}
