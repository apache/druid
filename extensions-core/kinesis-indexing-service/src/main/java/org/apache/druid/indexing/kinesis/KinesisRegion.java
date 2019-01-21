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

package org.apache.druid.indexing.kinesis;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.EnumSet;
import java.util.List;
import java.util.stream.Collectors;

public enum KinesisRegion
{
  US_EAST_2,
  US_EAST_1,
  US_WEST_1,
  US_WEST_2,
  AP_NORTHEAST_1,
  AP_NORTHEAST_2,
  AP_NORTHEAST_3,
  AP_SOUTH_1,
  AP_SOUTHEAST_1,
  AP_SOUTHEAST_2,
  CA_CENTRAL_1,
  CN_NORTH_1,
  CN_NORTHWEST_1,
  EU_CENTRAL_1,
  EU_WEST_1,
  EU_WEST_2,
  EU_WEST_3,
  SA_EAST_1,
  US_GOV_WEST_1;

  @JsonCreator
  public static KinesisRegion fromString(String value)
  {
    return EnumSet.allOf(KinesisRegion.class)
                  .stream()
                  .filter(x -> x.serialize().equals(value))
                  .findFirst()
                  .orElseThrow(() -> new IAE("Invalid region %s, region must be one of: %s", value, getNames()));
  }

  private static List<String> getNames()
  {
    return EnumSet.allOf(KinesisRegion.class).stream().map(KinesisRegion::serialize).collect(Collectors.toList());
  }

  public String getEndpoint()
  {
    return StringUtils.format("kinesis.%s.amazonaws.com%s", serialize(), serialize().startsWith("cn-") ? ".cn" : "");
  }

  @JsonValue
  public String serialize()
  {
    return StringUtils.toLowerCase(name()).replace('_', '-');
  }
}
