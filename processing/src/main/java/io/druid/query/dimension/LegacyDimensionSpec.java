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

package io.druid.query.dimension;

import com.fasterxml.jackson.annotation.JsonCreator;
import io.druid.java.util.common.IAE;

import java.util.Map;

/**
 */
public class LegacyDimensionSpec extends DefaultDimensionSpec
{
  private static final String convertValue(Object dimension, String name)
  {
    final String retVal;

    if (dimension instanceof String) {
      retVal = (String) dimension;
    } else if (dimension instanceof Map) {
      retVal = (String) ((Map) dimension).get(name);
    } else {
      throw new IAE("Unknown type[%s] for dimension[%s]", dimension.getClass(), dimension);
    }

    return retVal;
  }

  @JsonCreator
  public LegacyDimensionSpec(Object dimension)
  {
    super(convertValue(dimension, "dimension"), convertValue(dimension, "outputName"));
  }
}
