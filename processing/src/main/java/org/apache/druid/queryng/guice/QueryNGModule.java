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

package org.apache.druid.queryng.guice;

import com.google.inject.Binder;
import com.google.inject.Module;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.queryng.config.QueryNGConfig;
import org.apache.druid.queryng.fragment.QueryManagerFactory;
import org.apache.druid.queryng.fragment.QueryManagerFactoryImpl;

/**
 * Configure the "shim" version of the NG query engine which entails
 * creating a config (to enable or disable the engine) and to create
 * a factory for the fragment context. In this early version, all
 * other parts of the engine are distributed across various query
 * runners.
 */
public class QueryNGModule implements Module
{
  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, QueryNGConfig.CONFIG_ROOT, QueryNGConfig.class);
    binder
      .bind(QueryManagerFactory.class)
      // Query NG disabled in production nodes for now.
      //.to(NullFragmentBuilderFactory.class)
      // Enabled here just for debugging.
      .to(QueryManagerFactoryImpl.class)
      .in(LazySingleton.class);
  }
}
