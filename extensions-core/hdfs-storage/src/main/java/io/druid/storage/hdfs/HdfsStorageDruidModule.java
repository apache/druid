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

package io.druid.storage.hdfs;

import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.Module;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.multibindings.MapBinder;

import io.druid.data.SearchableVersionedDataFinder;
import io.druid.guice.Binders;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.LazySingleton;
import io.druid.guice.LifecycleModule;
import io.druid.guice.ManageLifecycle;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.logger.Logger;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogs;
import io.druid.storage.hdfs.tasklog.HdfsTaskLogsConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

/**
 */
public class HdfsStorageDruidModule implements DruidModule
{
  private static final Logger log = new Logger(HdfsStorageDruidModule.class);
  public static final String SCHEME = "hdfs";
  private Properties props = null;

  @Inject
  public void setProperties(Properties props)
  {
    this.props = props;
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.of(
        new Module()
        {
          @Override
          public String getModuleName()
          {
            return "DruidHDFSStorage-" + System.identityHashCode(this);
          }

          @Override
          public Version version()
          {
            return Version.unknownVersion();
          }

          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(HdfsLoadSpec.class);
          }
        }
    );
  }

  @Override
  public void configure(Binder binder)
  {
    MapBinder.newMapBinder(binder, String.class, SearchableVersionedDataFinder.class)
             .addBinding(SCHEME)
             .to(HdfsFileTimestampVersionFinder.class)
             .in(LazySingleton.class);

    Binders.dataSegmentPullerBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentPuller.class).in(LazySingleton.class);
    Binders.dataSegmentPusherBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentPusher.class).in(LazySingleton.class);
    Binders.dataSegmentKillerBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentKiller.class).in(LazySingleton.class);
    Binders.dataSegmentFinderBinder(binder).addBinding(SCHEME).to(HdfsDataSegmentFinder.class).in(LazySingleton.class);

    final Configuration conf = new Configuration();

    // Set explicit CL. Otherwise it'll try to use thread context CL, which may not have all of our dependencies.
    conf.setClassLoader(getClass().getClassLoader());

    // Ensure that FileSystem class level initialization happens with correct CL
    // See https://github.com/druid-io/druid/issues/1714
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
      FileSystem.get(conf);
    }
    catch (IOException ex) {
      throw Throwables.propagate(ex);
    }
    finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }

    if (props != null) {
      for (String propName : props.stringPropertyNames()) {
        if (propName.startsWith("hadoop.")) {
          conf.set(propName.substring("hadoop.".length()), props.getProperty(propName));
        }
      }
    }

    binder.bind(Configuration.class).toInstance(conf);
    JsonConfigProvider.bind(binder, "druid.storage", HdfsDataSegmentPusherConfig.class);

    Binders.taskLogsBinder(binder).addBinding("hdfs").to(HdfsTaskLogs.class);
    JsonConfigProvider.bind(binder, "druid.indexer.logs", HdfsTaskLogsConfig.class);
    binder.bind(HdfsTaskLogs.class).in(LazySingleton.class);
    JsonConfigProvider.bind(binder, "druid.hadoop.security.kerberos", HdfsKerberosConfig.class);
    binder.bind(HdfsStorageAuthentication.class).in(ManageLifecycle.class);
    LifecycleModule.register(binder, HdfsStorageAuthentication.class);

  }
}
