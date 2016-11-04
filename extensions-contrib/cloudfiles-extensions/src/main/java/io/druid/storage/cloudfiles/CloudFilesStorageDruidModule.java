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

package io.druid.storage.cloudfiles;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Provides;

import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.logger.Logger;

import org.jclouds.ContextBuilder;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.openstack.v2_0.config.InternalUrlModule;
import org.jclouds.osgi.ProviderRegistry;
import org.jclouds.rackspace.cloudfiles.uk.CloudFilesUKProviderMetadata;
import org.jclouds.rackspace.cloudfiles.us.CloudFilesUSProviderMetadata;
import org.jclouds.rackspace.cloudfiles.v1.CloudFilesApi;

import java.util.List;

/**
 */
public class CloudFilesStorageDruidModule implements DruidModule
{

  private static final Logger log = new Logger(CloudFilesStorageDruidModule.class);
  public static final String SCHEME = "cloudfiles";

  @Override
  public List<? extends Module> getJacksonModules()
  {
    log.info("Getting jackson modules...");

    return ImmutableList.of(
        new Module()
        {
          @Override
          public String getModuleName()
          {
            return "CloudFiles-" + System.identityHashCode(this);
          }

          @Override
          public Version version()
          {
            return Version.unknownVersion();
          }

          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(CloudFilesLoadSpec.class);
          }
        }
    );
  }

  @Override
  public void configure(Binder binder)
  {
    log.info("Configuring CloudFilesStorageDruidModule...");
    JsonConfigProvider.bind(binder, "druid.storage", CloudFilesDataSegmentPusherConfig.class);
    JsonConfigProvider.bind(binder, "druid.cloudfiles", CloudFilesAccountConfig.class);

    Binders.dataSegmentPullerBinder(binder).addBinding(SCHEME).to(CloudFilesDataSegmentPuller.class)
           .in(LazySingleton.class);
    Binders.dataSegmentPusherBinder(binder).addBinding(SCHEME).to(CloudFilesDataSegmentPusher.class)
           .in(LazySingleton.class);

    log.info("Configured CloudFilesStorageDruidModule.");
  }

  @Provides
  @LazySingleton
  public CloudFilesApi getCloudFilesApi(final CloudFilesAccountConfig config)
  {
    log.info("Building Cloud Files Api...");

    Iterable<com.google.inject.Module> modules = null;
    if (config.getUseServiceNet()) {
      log.info("Configuring Cloud Files Api to use the internal service network...");
      modules = ImmutableSet.<com.google.inject.Module>of(new SLF4JLoggingModule(), new InternalUrlModule());
    } else {
      log.info("Configuring Cloud Files Api to use the public network...");
      modules = ImmutableSet.<com.google.inject.Module>of(new SLF4JLoggingModule());
    }

    ProviderRegistry.registerProvider(CloudFilesUSProviderMetadata.builder().build());
    ProviderRegistry.registerProvider(CloudFilesUKProviderMetadata.builder().build());
    ContextBuilder cb = ContextBuilder.newBuilder(config.getProvider())
                                      .credentials(config.getUserName(), config.getApiKey()).modules(modules);
    CloudFilesApi cfa = cb.buildApi(CloudFilesApi.class);
    log.info("Cloud Files Api built.");
    return cfa;
  }

}
