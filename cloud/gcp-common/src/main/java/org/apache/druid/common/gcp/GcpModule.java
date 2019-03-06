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

package org.apache.druid.common.gcp;

import com.fasterxml.jackson.databind.Module;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.initialization.DruidModule;

import java.io.IOException;
import java.security.GeneralSecurityException;
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
    // Nothing to proactively bind
  }


  @Provides
  @LazySingleton
  public HttpRequestInitializer getHttpRequestInitializer(HttpTransport transport, JsonFactory factory)
      throws IOException
  {
    GoogleCredential credential = GoogleCredential.getApplicationDefault(transport, factory);
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(Collections.singleton("https://www.googleapis.com/auth/cloud-platform"));
    }
    return credential;
  }

  @Provides
  @LazySingleton
  public HttpTransport getHttpTransport() throws GeneralSecurityException, IOException
  {
    return GoogleNetHttpTransport.newTrustedTransport();
  }

  @Provides
  @LazySingleton
  public JsonFactory getJsonFactory()
  {
    return JacksonFactory.getDefaultInstance();
  }
}
