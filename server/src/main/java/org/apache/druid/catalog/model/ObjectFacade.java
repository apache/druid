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

package org.apache.druid.catalog.model;

import java.util.List;
import java.util.Map;

/**
 * Utility class to simplify typed access to catalog object properties.
 */
public abstract class ObjectFacade
{
  public abstract Map<String, Object> properties();

  public Object property(String key)
  {
    return properties().get(key);
  }

  public boolean hasProperty(String key)
  {
    return properties().containsKey(key);
  }

  public boolean booleanProperty(String key)
  {
    return (Boolean) property(key);
  }

  public String stringProperty(String key)
  {
    return (String) property(key);
  }

  public Integer intProperty(String key)
  {
    return (Integer) property(key);
  }

  @SuppressWarnings("unchecked")
  public List<String> stringListProperty(String key)
  {
    return (List<String>) property(key);
  }
}
