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

package org.apache.druid.msq.dart.controller;

import com.google.inject.Inject;
import org.apache.druid.messages.client.MessageListener;
import org.apache.druid.msq.dart.controller.messages.ControllerMessage;
import org.apache.druid.msq.dart.worker.WorkerId;
import org.apache.druid.msq.exec.Controller;
import org.apache.druid.msq.indexing.error.MSQErrorReport;
import org.apache.druid.server.DruidNode;

/**
 * Listener for worker-to-controller messages.
 * Also responsible for calling {@link Controller#workerError(MSQErrorReport)} when a worker server goes away.
 */
public class ControllerMessageListener implements MessageListener<ControllerMessage>
{
  private final DartControllerRegistry controllerRegistry;

  @Inject
  public ControllerMessageListener(final DartControllerRegistry controllerRegistry)
  {
    this.controllerRegistry = controllerRegistry;
  }

  @Override
  public void messageReceived(ControllerMessage message)
  {
    final ControllerHolder holder = controllerRegistry.get(message.getQueryId());
    if (holder != null) {
      message.handle(holder.getController());
    }
  }

  @Override
  public void serverAdded(DruidNode node)
  {
    // Nothing to do.
  }

  @Override
  public void serverRemoved(DruidNode node)
  {
    for (final ControllerHolder holder : controllerRegistry.getAllHolders()) {
      final Controller controller = holder.getController();
      final WorkerId workerId = WorkerId.fromDruidNode(node, controller.queryId());
      holder.workerOffline(workerId);
    }
  }
}
