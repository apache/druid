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

package org.apache.druid.testing.embedded;

import com.google.inject.Binder;
import com.google.inject.Inject;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.DruidProcessingConfigTest;
import org.apache.druid.utils.RuntimeInfo;

import java.util.Properties;

/**
 * Module for providing overridden {@link RuntimeInfo}, based on {@link EmbeddedDruidServer#setServerMemory(long)}
 * and {@link EmbeddedDruidServer#setServerDirectMemory(long)}.
 */
public class RuntimeInfoModule implements DruidModule
{
  public static final String SERVER_MEMORY_PROPERTY = "druid.testing.embedded.serverMemory";
  public static final String SERVER_DIRECT_MEMORY_PROPERTY = "druid.testing.embedded.serverDirectMemory";
  private static final int NUM_PROCESSORS = 2;

  private Properties properties;

  @Inject
  public void setProperties(Properties properties)
  {
    this.properties = properties;
  }

  @Override
  public void configure(final Binder binder)
  {
    final long serverMemory = Long.parseLong(properties.getProperty(SERVER_MEMORY_PROPERTY));
    final long serverDirectMemory = Long.parseLong(properties.getProperty(SERVER_DIRECT_MEMORY_PROPERTY));
    final DruidProcessingConfigTest.MockRuntimeInfo runtimeInfo =
        new DruidProcessingConfigTest.MockRuntimeInfo(
            NUM_PROCESSORS,
            serverDirectMemory,
            serverMemory
        );
    binder.bind(RuntimeInfo.class).toInstance(runtimeInfo);
  }
}
