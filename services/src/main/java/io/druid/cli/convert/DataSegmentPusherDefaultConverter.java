/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.cli.convert;

import com.google.api.client.util.Maps;
import com.google.common.collect.ImmutableMap;
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
    }
    else if (Boolean.parseBoolean(props.getProperty("druid.pusher.cassandra", "false"))) {
      type = "c*";
    }
    else if (Boolean.parseBoolean(props.getProperty("druid.pusher.hdfs", "false"))) {
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
