/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.client;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.metamx.common.IAE;
import com.metamx.common.Pair;
import com.metamx.common.guava.BaseSequence;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.common.logger.Logger;
import com.metamx.druid.Query;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.query.MetricManipulationFn;
import com.metamx.druid.query.QueryRunner;
import com.metamx.druid.query.QueryToolChest;
import com.metamx.druid.query.QueryToolChestWarehouse;
import com.metamx.druid.result.BySegmentResultValueClass;
import com.metamx.druid.result.Result;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.io.AppendableByteArrayInputStream;
import com.metamx.http.client.response.ClientResponse;
import com.metamx.http.client.response.InputStreamResponseHandler;

import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 */
public class DirectDruidClient<T> implements QueryRunner<T>
{
  private static final Logger log = new Logger(DirectDruidClient.class);

  private static final Map<Class<? extends Query>, Pair<JavaType, JavaType>> typesMap = Maps.newConcurrentMap();

  private final QueryToolChestWarehouse warehouse;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;
  private final String host;

  private final boolean isSmile;

  public DirectDruidClient(
      QueryToolChestWarehouse warehouse,
      ObjectMapper objectMapper,
      HttpClient httpClient,
      String host
  )
  {
    this.warehouse = warehouse;
    this.objectMapper = objectMapper;
    this.httpClient = httpClient;
    this.host = host;

    isSmile = this.objectMapper.getJsonFactory() instanceof SmileFactory;
  }

  @Override
  public Sequence<T> run(Query<T> query)
  {
    QueryToolChest<T, Query<T>> toolChest = warehouse.getToolChest(query);
    boolean isBySegment = Boolean.parseBoolean(query.getContextValue("bySegment", "false"));

    Pair<JavaType, JavaType> types = typesMap.get(query.getClass());
    if (types == null) {
      final TypeFactory typeFactory = objectMapper.getTypeFactory();
      JavaType baseType = typeFactory.constructType(toolChest.getResultTypeReference());
      JavaType bySegmentType = typeFactory.constructParametricType(
          Result.class, typeFactory.constructParametricType(BySegmentResultValueClass.class, baseType)
      );
      types = Pair.of(baseType, bySegmentType);
      typesMap.put(query.getClass(), types);
    }

    final JavaType typeRef;
    if (isBySegment) {
      typeRef = types.rhs;
    }
    else {
      typeRef = types.lhs;
    }

    final Future<InputStream> future;
    final String url = String.format("http://%s/druid/v2/", host);

    try {
      log.debug("Querying url[%s]", url);
      future = httpClient
          .post(new URL(url))
          .setContent(objectMapper.writeValueAsBytes(query))
          .setHeader(HttpHeaders.Names.CONTENT_TYPE, isSmile ? "application/smile" : "application/json")
          .go(
              new InputStreamResponseHandler()
              {

                long startTime;
                long byteCount = 0;

                @Override
                public ClientResponse<AppendableByteArrayInputStream> handleResponse(HttpResponse response)
                {
                  log.debug("Initial response from url[%s]", url);
                  startTime = System.currentTimeMillis();
                  byteCount += response.getContent().readableBytes();
                  return super.handleResponse(response);
                }

                @Override
                public ClientResponse<AppendableByteArrayInputStream> handleChunk(
                    ClientResponse<AppendableByteArrayInputStream> clientResponse, HttpChunk chunk
                )
                {
                  final int bytes = chunk.getContent().readableBytes();
                  byteCount += bytes;
                  return super.handleChunk(clientResponse, chunk);
                }

                @Override
                public ClientResponse<InputStream> done(ClientResponse<AppendableByteArrayInputStream> clientResponse)
                {
                  long stopTime = System.currentTimeMillis();
                  log.debug(
                      "Completed request to url[%s] with %,d bytes returned in %,d millis [%,f b/s].",
                      url,
                      byteCount,
                      stopTime - startTime,
                      byteCount / (0.0001 * (stopTime - startTime))
                  );
                  return super.done(clientResponse);
                }
              }
          );
    }
    catch (IOException e) {
      throw Throwables.propagate(e);
    }

    Sequence<T> retVal = new BaseSequence<T, JsonParserIterator<T>>(
        new BaseSequence.IteratorMaker<T, JsonParserIterator<T>>()
        {
          @Override
          public JsonParserIterator<T> make()
          {
            return new JsonParserIterator<T>(typeRef, future);
          }

          @Override
          public void cleanup(JsonParserIterator<T> iterFromMake)
          {
            Closeables.closeQuietly(iterFromMake);
          }
        }
    );

    if (!isBySegment) {
      retVal = Sequences.map(
          retVal,
          toolChest.makeMetricManipulatorFn(query, new MetricManipulationFn()
          {
            @Override
            public Object manipulate(AggregatorFactory factory, Object object)
            {
              return factory.deserialize(object);
            }
          })
      );
    }

    return retVal;
  }

  private class JsonParserIterator<T> implements Iterator<T>, Closeable
  {
    private JsonParser jp;
    private ObjectCodec objectCodec;
    private final JavaType typeRef;
    private final Future<InputStream> future;

    public JsonParserIterator(JavaType typeRef, Future<InputStream> future)
    {
      this.typeRef = typeRef;
      this.future = future;
      jp = null;
    }

    @Override
    public boolean hasNext()
    {
      init();

      if (jp.isClosed()) {
        return false;
      }
      if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
        Closeables.closeQuietly(jp);
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
          jp = objectMapper.getJsonFactory().createJsonParser(future.get());
          if (jp.nextToken() != JsonToken.START_ARRAY) {
            throw new IAE("Next token wasn't a START_ARRAY, was[%s]", jp.getCurrentToken());
          } else {
            jp.nextToken();
            objectCodec = jp.getCodec();
          }
        }
        catch (IOException e) {
          throw Throwables.propagate(e);
        }
        catch (InterruptedException e) {
          throw Throwables.propagate(e);
        }
        catch (ExecutionException e) {
          throw Throwables.propagate(e);
        }
      }
    }

    @Override
    public void close() throws IOException
    {
      jp.close();
    }
  }
}
