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

package org.apache.druid.rpc.handler;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;

/**
 * Implementation of {@link ObjectMapperHttpResponseHandler} that reads Smile.
 */
public class SmileHttpResponseHandler<T> extends ObjectMapperHttpResponseHandler<T>
{
  private SmileHttpResponseHandler(ObjectMapperHttpResponseHandler.DeserializeFn<T> deserializeFn)
  {
    super(deserializeFn, SmileMediaTypes.APPLICATION_JACKSON_SMILE);
  }

  public static <T> SmileHttpResponseHandler<T> create(final ObjectMapper jsonMapper, final Class<T> clazz)
  {
    return new SmileHttpResponseHandler<>(in -> jsonMapper.readValue(in, clazz));
  }

  public static <T> SmileHttpResponseHandler<T> create(final ObjectMapper jsonMapper, final TypeReference<T> typeRef)
  {
    return new SmileHttpResponseHandler<>(in -> jsonMapper.readValue(in, typeRef));
  }
}
