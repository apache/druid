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

import java.util.Map;
import java.util.Properties;

/**
 *
 */
public class ValueConverter implements PropertyConverter
{
  private final Map<String, String> valueMap;
  private final String property;
  public ValueConverter(String property, Map<String, String> valueMap){
    this.property = property;
    this.valueMap = valueMap;
  }

  @Override
  public boolean canHandle(String property)
  {
    return this.property.equals(property);
  }

  @Override
  public Map<String, String> convert(Properties properties)
  {
    final String oldValue = properties.getProperty(this.property);
    if(null == oldValue){
      return ImmutableMap.of();
    }
    final String newValue = valueMap.get(oldValue);
    if(null == newValue){
      return ImmutableMap.of();
    }
    return ImmutableMap.of(this.property, newValue);
  }
}
