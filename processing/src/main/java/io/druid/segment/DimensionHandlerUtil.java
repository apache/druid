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

import io.druid.java.util.common.IAE;
import io.druid.segment.column.ColumnCapabilities;
import io.druid.segment.column.ValueType;

public final class DimensionHandlerUtil
{
  private DimensionHandlerUtil() {}

  public static DimensionHandler getHandlerFromCapabilities(String dimensionName, ColumnCapabilities capabilities)
  {
    DimensionHandler handler = null;
    if (capabilities.getType() == ValueType.STRING) {
      if (!capabilities.isDictionaryEncoded() || !capabilities.hasBitmapIndexes()) {
        throw new IAE("String column must have dictionary encoding and bitmap index.");
      }
      handler = new StringDimensionHandler(dimensionName);
    }
    if (handler == null) {
      throw new IAE("Could not create handler from invalid column type: " + capabilities.getType());
    }
    return handler;
  }
}
