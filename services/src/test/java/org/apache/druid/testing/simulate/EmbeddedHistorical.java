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

package org.apache.druid.testing.simulate;

import com.google.inject.Injector;
import com.google.inject.Module;
import org.apache.druid.cli.CliHistorical;
import org.apache.druid.cli.ServerRunnable;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.utils.RuntimeInfo;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Embedded mode of {@link CliHistorical} used in simulation tests.
 */
public class EmbeddedHistorical extends EmbeddedDruidServer
{
  private static final long MEM_100_MB = 100_000_000;

  @Override
  ServerRunnable createRunnable(LifecycleInitHandler handler)
  {
    return new Historical(handler);
  }

  @Override
  RuntimeInfo getRuntimeInfo()
  {
    return new DruidProcessingConfigTest.MockRuntimeInfo(2, MEM_100_MB, MEM_100_MB);
  }

  @Override
  Properties buildStartupProperties(
      TestFolder testFolder,
      EmbeddedZookeeper zk,
      @Nullable TestDerbyConnector.DerbyConnectorRule dbRule
  ) throws IOException
  {
    final Properties properties = super.buildStartupProperties(testFolder, zk, dbRule);
    properties.setProperty(
        "druid.segmentCache.locations",
        StringUtils.format(
            "[{\"path\":\"%s\",\"maxSize\":\"%s\"}]",
            testFolder.newFolder().getAbsolutePath(),
            MEM_100_MB
        )
    );
    return properties;
  }

  private static class Historical extends CliHistorical
  {
    private final LifecycleInitHandler handler;

    private Historical(LifecycleInitHandler handler)
    {
      this.handler = handler;
    }

    @Override
    public Lifecycle initLifecycle(Injector injector)
    {
      final Lifecycle lifecycle = super.initLifecycle(injector);
      handler.onLifecycleInit(lifecycle);
      return lifecycle;
    }

    @Override
    protected List<? extends Module> getModules()
    {
      final List<Module> modules = new ArrayList<>(handler.getInitModules());
      modules.addAll(super.getModules());
      return modules;
    }
  }
}
