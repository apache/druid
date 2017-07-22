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

package io.druid.cli.convert;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 */
public class DataSegmentPusherDefaultConverter implements PropertyConverter
{
  Set<String> propertiesHandled = Sets.newHashSet("druid.pusher.local", "druid.pusher.cassandra", "druid.pusher.hdfs");

  @Override
  public boolean canHandle(String property)
  {
    return propertiesHandled.contains(property) || property.startsWith("druid.pusher.s3");
  }

  @Override
  public Map<String, String> convert(Properties props)
  {
    String type = null;
    if (Boolean.parseBoolean(props.getProperty("druid.pusher.local", "false"))) {
      type = "local";
    } else if (Boolean.parseBoolean(props.getProperty("druid.pusher.cassandra", "false"))) {
      type = "c*";
    } else if (Boolean.parseBoolean(props.getProperty("druid.pusher.hdfs", "false"))) {
      type = "hdfs";
    }

    if (type != null) {
      return ImmutableMap.of("druid.storage.type", type);
    }

    // It's an s3 property, which means we need to set the type and convert the other values.
    Map<String, String> retVal = Maps.newHashMap();

    retVal.put("druid.pusher.type", type);
    retVal.putAll(new Rename("druid.pusher.s3.bucket", "druid.storage.bucket").convert(props));
    retVal.putAll(new Rename("druid.pusher.s3.baseKey", "druid.storage.baseKey").convert(props));
    retVal.putAll(new Rename("druid.pusher.s3.disableAcl", "druid.storage.disableAcl").convert(props));

    return retVal;
  }
}
