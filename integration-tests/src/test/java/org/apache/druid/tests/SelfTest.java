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

package org.apache.druid.tests;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.IndexingServiceFirehoseModule;
import org.apache.druid.guice.IndexingServiceInputSourceModule;
import org.apache.druid.guice.IndexingServiceTuningConfigModule;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.BytesFullResponseHolder;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.apache.druid.testing.clients.QueryResourceTestClient;
import org.apache.druid.testing.guice.DruidTestModule;
import org.apache.druid.testing.guice.TestClient;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.jboss.netty.buffer.ByteBufferBackedChannelBuffer;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.joda.time.Duration;
import org.testng.IModuleFactory;
import org.testng.ITestContext;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.ws.rs.core.MediaType;

import static org.testng.Assert.assertEquals;

@Guice(moduleFactory = SelfTest.LocalModuleFactory.class)
public class SelfTest
{

  static class LocalModule implements Module
  {

    DruidTestModule delegate = new DruidTestModule();

    @Override
    public void configure(Binder binder)
    {
      delegate.configure(binder);
    }

    @Provides
    @TestClient
    public HttpClient getHttpClient()
    {
      return new HttpClient()
      {
        @Override
        public <Intermediate, Final> ListenableFuture<Final> go(Request request,
            HttpResponseHandler<Intermediate, Final> handler)
        {
          @Nullable
          Final val = null;

          int counter = SelfTest.requestCounter.getAndIncrement();
          HttpResponse response = new DefaultHttpResponse(
              HttpVersion.HTTP_1_1,
              counter == 0 ? HttpResponseStatus.INTERNAL_SERVER_ERROR : HttpResponseStatus.OK)
          {
            public org.jboss.netty.buffer.ChannelBuffer getContent()
            {
              return new ByteBufferBackedChannelBuffer(ByteBuffer.wrap("[{}]".getBytes()));
            }
          };
          response.headers().add(HttpHeaders.Names.CONTENT_TYPE, MediaType.APPLICATION_JSON);

          BytesFullResponseHolder bytesFullResponseHolder = new BytesFullResponseHolder(response);
          bytesFullResponseHolder.addChunk("[{}]".getBytes());
          val = (@Nullable Final) bytesFullResponseHolder;
          return Futures.<Final> immediateFuture(val);
        }

        @Override
        public <Intermediate, Final> ListenableFuture<Final> go(Request request,
            HttpResponseHandler<Intermediate, Final> handler, Duration readTimeout)
        {
          throw new RuntimeException("Unimplemented!");
        }
      };
    }
  }

  static class LocalModuleFactory implements IModuleFactory
  {
    private static final Module MODULE = new DruidTestModule();
    private static final Injector INJECTOR = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        getModules());

    public static Injector getInjector()
    {
      return INJECTOR;
    }

    private static List<? extends Module> getModules()
    {
      return ImmutableList.of(
          new LocalModule(),
          new IndexingServiceFirehoseModule(),
          new IndexingServiceInputSourceModule(),
          new IndexingServiceTuningConfigModule());
    }

    @Override
    public Module createModule(ITestContext context, Class<?> testClass)
    {
      context.addInjector(Collections.singletonList(MODULE), INJECTOR);
      return MODULE;
    }
  }

  @Inject
  private QueryResourceTestClient queryClient;

  private static AtomicInteger requestCounter = new AtomicInteger();

  @Test
  public void testInternalServerErrorAtFirstTry() throws JsonProcessingException
  {

//List<Map<String, Object>> v=new ArrayList<Map<String,Object>>();
//v.add(new HashMap<String, Object>());
//String s = new ObjectMapper().writeValueAsString(v);
//if(true) {
//  throw new RuntimeException(s);
//}

    requestCounter.set(0);
    queryClient.query("http://192.168.99.99/asd", null);
    assertEquals(2, requestCounter.get());
  }

}
