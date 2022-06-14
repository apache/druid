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

package org.apache.druid.catalog;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.ISE;

import java.io.IOException;

public class CatalogDefns
{
  public static byte[] toBytes(ObjectMapper jsonMapper, Object obj)
  {
    try {
      return jsonMapper.writeValueAsBytes(obj);
    }
    catch (JsonProcessingException e) {
      throw new ISE("Failed to serialize " + obj.getClass().getSimpleName());
    }
  }

  public static <T> T fromBytes(ObjectMapper jsonMapper, byte[] bytes, Class<T> clazz)
  {
    try {
      return jsonMapper.readValue(bytes, clazz);
    }
    catch (IOException e) {
      throw new ISE(e, "Failed to deserialize a " + clazz.getSimpleName());
    }
  }

  public static String toString(Object obj)
  {
    ObjectMapper jsonMapper = new ObjectMapper();
    try {
      return jsonMapper.writeValueAsString(obj);
    }
    catch (JsonProcessingException e) {
      throw new ISE("Failed to serialize TableDefn");
    }
  }
}
