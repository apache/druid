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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.druid.query.LegacyBrokerParallelMergeConfig;

/**
 * Backwards compatibility for runtime.properties for Druid 27 and older to make deprecated config paths of
 * {@link LegacyBrokerParallelMergeConfig} still work for Druid 28.
 * {@link org.apache.druid.query.BrokerParallelMergeConfig} has replaced these configs, and will warn when these
 * deprecated paths are configured. This module should be removed in Druid 29, along with
 * {@link LegacyBrokerParallelMergeConfig} as well as the config-magic library that makes it work.
 */
@Deprecated
public class LegacyBrokerParallelMergeConfigModule implements Module
{

  @Override
  public void configure(Binder binder)
  {
    ConfigProvider.bind(binder, LegacyBrokerParallelMergeConfig.class);
  }
}
