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

package org.apache.druid.segment.serde;

import org.apache.druid.java.util.common.ISE;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class ComplexMetrics
{
  private static final Map<String, ComplexMetricSerde> COMPLEX_SERIALIZERS = new HashMap<>();

  @Nullable
  public static ComplexMetricSerde getSerdeForType(String type)
  {
    return COMPLEX_SERIALIZERS.get(type);
  }

  public static void registerSerde(String type, ComplexMetricSerde serde)
  {
    if (COMPLEX_SERIALIZERS.containsKey(type)) {
      if (!COMPLEX_SERIALIZERS.get(type).getClass().getName().equals(serde.getClass().getName())) {
        throw new ISE(
            "Incompatible serializer for type[%s] already exists. Expected [%s], found [%s].",
            type,
            serde.getClass().getName(),
            COMPLEX_SERIALIZERS.get(type).getClass().getName()
        );
      }
    } else {
      COMPLEX_SERIALIZERS.put(type, serde);
    }
  }
}
