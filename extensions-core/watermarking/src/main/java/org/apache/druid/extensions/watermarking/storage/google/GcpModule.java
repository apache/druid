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

package org.apache.druid.extensions.watermarking.storage.google;

import com.fasterxml.jackson.databind.Module;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class GcpModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of();
  }

  @Override
  public void configure(Binder binder)
  {

  }


  @Provides
  @LazySingleton
  public HttpRequestInitializer getHttpRequestInitializer(
      HttpTransport transport,
      JsonFactory factory
  )
  {
    try {
      return GoogleCredential
          .getApplicationDefault(transport, factory)
          .createScoped(Collections.singleton("https://www.googleapis.com/auth/cloud-platform"));
    }
    catch (IOException e) {
      throw new RuntimeException("Unable to build authentication", e);
    }
  }

  @Provides
  @LazySingleton
  public HttpTransport getHttpTransport()
  {
    return new NetHttpTransport.Builder()
        .build();
  }

  @Provides
  @LazySingleton
  public JsonFactory getJsonFactory()
  {
    return JacksonFactory.getDefaultInstance();
  }
}
