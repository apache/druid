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

package io.druid.storage.oss;

import com.aliyun.oss.OSSClient;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Provides;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;

import java.util.List;

/**
 */
public class OssStorageDruidModule implements DruidModule {

    public static final String SCHEME = "oss";

    @Override
    public List<? extends Module> getJacksonModules() {
        return ImmutableList.of(
                new Module() {
                    @Override
                    public String getModuleName() {
                        return "DruidOss-" + System.identityHashCode(this);
                    }

                    @Override
                    public Version version() {
                        return Version.unknownVersion();
                    }

                    @Override
                    public void setupModule(SetupContext context) {
                        context.registerSubtypes(OssLoadSpec.class);
                    }
                }
        );
    }

    @Override
    public void configure(Binder binder) {

        JsonConfigProvider.bind(binder, "druid.oss", OssCredentialsConfig.class);

        Binders.dataSegmentPullerBinder(binder).addBinding(SCHEME).to(OssDataSegmentPuller.class).in(LazySingleton.class);
        Binders.dataSegmentKillerBinder(binder).addBinding(SCHEME).to(OssDataSegmentKiller.class).in(LazySingleton.class);
        Binders.dataSegmentMoverBinder(binder).addBinding(SCHEME).to(OssDataSegmentMover.class).in(LazySingleton.class);
        Binders.dataSegmentArchiverBinder(binder).addBinding(SCHEME).to(OssDataSegmentArchiver.class).in(LazySingleton.class);
        Binders.dataSegmentPusherBinder(binder).addBinding(SCHEME).to(OssDataSegmentPusher.class).in(LazySingleton.class);
        Binders.taskLogsBinder(binder).addBinding(SCHEME).to(OssTaskLogs.class).in(LazySingleton.class);

        JsonConfigProvider.bind(binder, "druid.oss.storage", OssDataSegmentPusherConfig.class);
        JsonConfigProvider.bind(binder, "druid.oss.storage", OssDataSegmentArchiverConfig.class);
        JsonConfigProvider.bind(binder, "druid.oss.storage", OssTaskLogsConfig.class);

    }

    @Provides
    @LazySingleton
    public OSSClient getOSSClient(OssCredentialsConfig config) {

        return new OSSClient(config.getEndpoint(), config.getAccessKey(), config.getSecretKey());
    }
}