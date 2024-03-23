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
import org.apache.druid.client.FilteredServerInventoryView;
import org.apache.druid.client.FilteredServerInventoryViewProvider;
import org.apache.druid.client.HttpServerInventoryViewConfig;
import org.apache.druid.client.InventoryView;
import org.apache.druid.client.ServerInventoryView;
import org.apache.druid.client.ServerInventoryViewProvider;
import org.apache.druid.client.ServerView;

import java.util.StringJoiner;

/**
 */
public class ServerViewModule implements Module
{
  public static final String SERVERVIEW_PREFIX = "druid.serverview";
  public static final String TYPE = "type";
  public static final String SERVERVIEW_TYPE_PROPERTY = "druid.serverview.type";
  public static final String SERVERVIEW_TYPE_HTTP = "http";
  public static final String SERVERVIEW_TYPE_BATCH = "batch";

  // this value should be consistent with the default implementation used in
  // {@code ServerInventoryViewProvider} & {@code FilteredServerInventoryViewProvider}
  public static final String DEFAULT_SERVERVIEW_TYPE = "http";

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, SERVERVIEW_PREFIX, ServerInventoryViewProvider.class);
    JsonConfigProvider.bind(binder, SERVERVIEW_PREFIX, FilteredServerInventoryViewProvider.class);
    JsonConfigProvider.bind(
        binder,
        new StringJoiner(".", SERVERVIEW_PREFIX, SERVERVIEW_TYPE_HTTP).toString(),
        HttpServerInventoryViewConfig.class);
    binder.bind(InventoryView.class).to(ServerInventoryView.class);
    binder.bind(ServerView.class).to(ServerInventoryView.class);
    binder.bind(ServerInventoryView.class).toProvider(ServerInventoryViewProvider.class).in(ManageLifecycle.class);
    binder.bind(FilteredServerInventoryView.class)
          .toProvider(FilteredServerInventoryViewProvider.class)
          .in(ManageLifecycle.class);
  }
}
