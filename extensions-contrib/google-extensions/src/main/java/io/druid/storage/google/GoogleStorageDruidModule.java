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

package io.druid.storage.google;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.StorageScopes;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import io.druid.firehose.google.StaticGoogleBlobStoreFirehoseFactory;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.logger.Logger;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

public class GoogleStorageDruidModule implements DruidModule
{
  private static final Logger LOG = new Logger(GoogleStorageDruidModule.class);

  public static final String SCHEME = "google";
  private static final String APPLICATION_NAME = "druid-google-extensions";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    LOG.info("Getting jackson modules...");

    return ImmutableList.of(
        new Module()
        {
          @Override
          public String getModuleName()
          {
            return "Google-" + System.identityHashCode(this);
          }

          @Override
          public Version version()
          {
            return Version.unknownVersion();
          }

          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(GoogleLoadSpec.class);
          }
        },
        new SimpleModule().registerSubtypes(
            new NamedType(StaticGoogleBlobStoreFirehoseFactory.class, "static-google-blobstore"))
    );
  }

  @Override
  public void configure(Binder binder)
  {
    LOG.info("Configuring GoogleStorageDruidModule...");

    JsonConfigProvider.bind(binder, "druid.google", GoogleAccountConfig.class);

    Binders.dataSegmentPusherBinder(binder).addBinding(SCHEME).to(GoogleDataSegmentPusher.class)
           .in(LazySingleton.class);
    Binders.dataSegmentKillerBinder(binder).addBinding(SCHEME).to(GoogleDataSegmentKiller.class)
           .in(LazySingleton.class);
    Binders.dataSegmentFinderBinder(binder).addBinding(SCHEME).to(GoogleDataSegmentFinder.class)
           .in(LazySingleton.class);

    Binders.taskLogsBinder(binder).addBinding(SCHEME).to(GoogleTaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", GoogleTaskLogsConfig.class);
    binder.bind(GoogleTaskLogs.class).in(LazySingleton.class);
  }

  @Provides
  @LazySingleton
  public GoogleStorage getGoogleStorage(final GoogleAccountConfig config)
      throws IOException, GeneralSecurityException
  {
    LOG.info("Building Cloud Storage Client...");

    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    GoogleCredential credential = GoogleCredential.getApplicationDefault(httpTransport, jsonFactory);
    if (credential.createScopedRequired()) {
      credential = credential.createScoped(StorageScopes.all());
    }
    Storage storage = new Storage.Builder(httpTransport, jsonFactory, credential).setApplicationName(APPLICATION_NAME).build();

    return new GoogleStorage(storage);
  }
}
