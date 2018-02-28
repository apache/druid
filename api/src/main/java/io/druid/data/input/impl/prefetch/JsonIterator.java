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
package io.druid.data.input.impl.prefetch;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import io.druid.java.util.common.guava.CloseQuietly;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

public class JsonIterator<T> implements Iterator<T>, Closeable
{
  private JsonParser jp;
  private ObjectCodec objectCodec;
  private final TypeReference typeRef;
  private final InputStream inputStream;
  private final Closeable resourceCloser;
  private final ObjectMapper objectMapper;

  public JsonIterator(
      TypeReference typeRef,
      InputStream inputStream,
      Closeable resourceCloser,
      ObjectMapper objectMapper
  )
  {
    this.typeRef = typeRef;
    this.inputStream = inputStream;
    this.resourceCloser = resourceCloser;
    jp = null;
    this.objectMapper = objectMapper;
  }

  @Override
  public boolean hasNext()
  {
    init();
    if (jp.isClosed()) {
      return false;
    }
    if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
      CloseQuietly.close(jp);
      return false;
    }
    return true;
  }

  @Override
  public T next()
  {
    init();

    try {
      final T retVal = objectCodec.readValue(jp, typeRef);
      jp.nextToken();
      return retVal;
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }
  }


  @Override
  public void remove()
  {
    throw new UnsupportedOperationException();
  }

  private void init()
  {
    if (jp == null) {
      try {
        if (inputStream == null) {
          throw new UnsupportedOperationException();
        } else {
          jp = objectMapper.getFactory().createParser(inputStream);
        }
        objectCodec = jp.getCodec();
        jp.nextToken();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void close() throws IOException
  {
    if (jp != null) {
      jp.close();
    }
    try {
      resourceCloser.close();
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
