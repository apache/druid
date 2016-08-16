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

package io.druid.client;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Closeable;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Future;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = V2QueryResponseIteratorFactory.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "v2", value = V2QueryResponseIteratorFactory.class),
    @JsonSubTypes.Type(name = "v3", value = V3QueryResponseIteratorFactory.class)
})
interface QueryResponseIteratorFactory
{
  String getQueryUrlPath();

  QueryResponseIterator make(
      JavaType typeRef,
      Future<InputStream> future,
      String url,
      ObjectMapper objectMapper,
      String host,
      Map<String, Object> responseContext
  );
}

// marker interface for QueryResponseIterator for the response received from historicals/realtime-tasks
interface QueryResponseIterator<T> extends Iterator<T>, Closeable
{

}
