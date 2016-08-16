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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.metamx.common.IAE;
import com.metamx.common.RE;
import io.druid.query.QueryInterruptedException;
import io.druid.server.QueryResourceV3;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 */
class V3QueryResponseIterator<T> implements QueryResponseIterator<T>
{
  private JsonParser jp;
  private ObjectCodec objectCodec;
  private final JavaType typeRef;
  private final Future<InputStream> future;
  private final String url;

  private final ObjectMapper objectMapper;
  private final String host;
  private final Map<String, Object> responseContext;

  V3QueryResponseIterator(
      JavaType typeRef,
      Future<InputStream> future,
      String url,
      ObjectMapper objectMapper,
      String host,
      Map<String, Object> responseContext
  )
  {
    this.typeRef = typeRef;
    this.future = future;
    this.url = url;
    jp = null;
    this.objectMapper = objectMapper;
    this.host = host;
    this.responseContext = responseContext;
  }

  @Override
  public boolean hasNext()
  {
    init();

    if (jp.isClosed()) {
      return false;
    }

    if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
      // now we are finised reading all the result values.
      try {
        jp.nextToken(); //read off FIELD_NAME token for "context"
        jp.nextToken();
        Map<String, Object> ctx = objectCodec.readValue(jp, Map.class);
        if (ctx.size() > 0) {
          responseContext.put(host, ctx);
        }
        jp.nextToken(); //read off END_OBJECT token
      } catch (IOException ex) {
        throw Throwables.propagate(ex);
      }

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
        jp = objectMapper.getFactory().createParser(future.get());
        if (jp.nextToken() == JsonToken.START_OBJECT) {
          if (jp.nextToken() == JsonToken.FIELD_NAME) {
            if (QueryResourceV3.KEY_RESULT.equals(jp.getCurrentName())) {
              if (jp.nextToken() == JsonToken.START_ARRAY) {
                jp.nextToken();
                objectCodec = jp.getCodec();
              } else {
                throw new IAE("result must be array, token was[%s] from url [%s]", jp.getCurrentToken(), url);
              }
            } else {
              QueryInterruptedException cause = jp.getCodec().readValue(jp, QueryInterruptedException.class);
              throw new QueryInterruptedException(cause, host);
            }
          } else {
            throw new IAE("Next token wasn't a FIELD_NAME, was[%s] from url [%s]", jp.getCurrentToken(), url);
          }
        } else {
          throw new IAE("expecting json object, was[%s] from url [%s]", jp.getCurrentToken(), url);
        }
      }
      catch (IOException | InterruptedException | ExecutionException e) {
        throw new RE(e, "Failure getting results from[%s] because of [%s]", url, e.getMessage());
      }
      catch (CancellationException e) {
        throw new QueryInterruptedException(e, host);
      }
    }
  }

  @Override
  public void close() throws IOException
  {
    if (jp != null) {
      jp.close();
    }
  }
}

class V3QueryResponseIteratorFactory implements QueryResponseIteratorFactory
{
  @Override
  public String getQueryUrlPath()
  {
    return "/druid/v3";
  }

  @Override
  public QueryResponseIterator make(
      JavaType typeRef,
      Future<InputStream> future,
      String url,
      ObjectMapper objectMapper,
      String host,
      Map<String, Object> responseContext
  )
  {
    return null;
  }
}

