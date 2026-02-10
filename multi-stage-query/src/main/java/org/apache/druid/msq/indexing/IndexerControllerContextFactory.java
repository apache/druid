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

package org.apache.druid.msq.indexing;

import com.google.inject.Injector;
import org.apache.druid.indexing.common.TaskToolbox;
import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.rpc.ServiceClientFactory;
import org.apache.druid.rpc.indexing.OverlordClient;

/**
 * Factory for creating {@link IndexerControllerContext} instances.
 */
public class IndexerControllerContextFactory
{
  private final Injector injector;
  private final ServiceClientFactory clientFactory;
  private final OverlordClient overlordClient;

  public IndexerControllerContextFactory(
      final Injector injector,
      final ServiceClientFactory clientFactory,
      final OverlordClient overlordClient
  )
  {
    this.injector = injector;
    this.clientFactory = clientFactory;
    this.overlordClient = overlordClient;
  }

  public ControllerContext buildWithTask(MSQControllerTask task, TaskToolbox toolbox)
  {
    return new IndexerControllerContext(task, toolbox, injector, clientFactory, overlordClient);
  }
}
