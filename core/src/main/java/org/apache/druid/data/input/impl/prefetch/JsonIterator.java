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

package org.apache.druid.data.input.impl.prefetch;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.guava.CloseQuietly;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.java.util.common.parsers.CloseableIterator;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.NoSuchElementException;

/**
 * An iterator over an array of JSON objects. Uses {@link ObjectCodec} to deserialize regular Java objects.
 *
 * @param <T> the type of object returned by this iterator
 */
public class JsonIterator<T> implements CloseableIterator<T>
{
  private JsonParser jp;
  private ObjectCodec objectCodec;
  private final TypeReference<T> typeRef;
  private final InputStream inputStream;
  private final Closeable resourceCloser;
  private final ObjectMapper objectMapper;

  /**
   * @param typeRef        the object type that the JSON object should be deserialized into
   * @param inputStream    stream containing an array of JSON objects
   * @param resourceCloser a {@code Closeable} implementation to release resources that the object is holding
   * @param objectMapper   object mapper, used for deserialization
   */
  public JsonIterator(
      TypeReference<T> typeRef,
      InputStream inputStream,
      Closeable resourceCloser,
      ObjectMapper objectMapper
  )
  {
    this.typeRef = typeRef;
    this.inputStream = inputStream;
    this.resourceCloser = resourceCloser;
    this.objectMapper = objectMapper;
    init();
  }

  /**
   * Returns {@code true} if there are more objects to be read.
   *
   * @return {@code true} if there are more objects to be read, else return {@code false}
   */
  @Override
  public boolean hasNext()
  {
    if (jp.isClosed()) {
      return false;
    }
    if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
      CloseQuietly.close(jp);
      return false;
    }
    return true;
  }

  /**
   * Retrieves the next deserialized object from the stream of JSON objects.
   *
   * @return the next deserialized object from the stream of JSON ovbjects
   */
  @Override
  public T next()
  {
    if (!hasNext()) {
      throw new NoSuchElementException("No more objects to read!");
    }
    try {
      final T retVal = objectCodec.readValue(jp, typeRef);
      jp.nextToken();
      return retVal;
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void init()
  {
    try {
      if (inputStream == null) {
        throw new UnsupportedOperationException();
      } else {
        jp = objectMapper.getFactory().createParser(inputStream);
      }
      final JsonToken nextToken = jp.nextToken();
      if (nextToken != JsonToken.START_ARRAY) {
        throw new IAE("First token should be START_ARRAY, but it is actually [%s]", jp.getCurrentToken());
      } else {
        jp.nextToken();
        objectCodec = jp.getCodec();
      }
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException
  {
    Closer closer = Closer.create();
    if (jp != null) {
      closer.register(jp);
    }
    closer.register(resourceCloser);
    closer.close();
  }
}
