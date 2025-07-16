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

package org.apache.druid.testing.embedded.docker;

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.DruidCommand;

/**
 * Factory for {@link DruidContainerResource} that can run specific services.
 *
 * @see #newOverlord()
 * @see #newCoordinator()
 */
public final class DruidContainers
{
  private DruidContainers()
  {
    // no instantiation
  }

  /**
   * Creates a new {@link DruidContainerResource} to run a Coordinator node.
   */
  public static DruidContainerResource newCoordinator()
  {
    return new DruidContainerResource(DruidCommand.COORDINATOR)
        .addProperty("druid.coordinator.startDelay", "PT0.1S")
        .addProperty("druid.coordinator.period", "PT0.5S")
        .addProperty("druid.manager.segments.pollDuration", "PT0.1S");
  }

  /**
   * Creates a new {@link DruidContainerResource} to run an Overlord node.
   */
  public static DruidContainerResource newOverlord()
  {
    // Keep a small sync timeout so that Peons and Indexers are not stuck
    // handling a change request when Overlord has already shutdown
    return new DruidContainerResource(DruidCommand.OVERLORD)
        .addProperty("druid.indexer.storage.type", "metadata")
        .addProperty("druid.indexer.queue.startDelay", "PT0S")
        .addProperty("druid.indexer.queue.restartDelay", "PT0S")
        .addProperty("druid.indexer.runner.syncRequestTimeout", "PT1S");
  }

  /**
   * Creates a new {@link DruidContainerResource} to run an Indexer node.
   */
  public static DruidContainerResource newIndexer()
  {
    return new DruidContainerResource(DruidCommand.INDEXER)
        .addProperty("druid.lookup.enableLookupSyncOnStartup", "false")
        .addProperty("druid.processing.buffer.sizeBytes", "50MiB")
        .addProperty("druid.processing.numMergeBuffers", "2")
        .addProperty("druid.processing.numThreads", "5");
  }

  /**
   * Creates a new {@link DruidContainerResource} to run a MiddleManager node.
   */
  public static DruidContainerResource newMiddleManager()
  {
    return new DruidContainerResource(DruidCommand.MIDDLE_MANAGER)
        .addProperty("druid.lookup.enableLookupSyncOnStartup", "false")
        .addProperty("druid.processing.buffer.sizeBytes", "50MiB")
        .addProperty("druid.processing.numMergeBuffers", "2")
        .addProperty("druid.processing.numThreads", "5");
  }

  /**
   * Creates a new {@link DruidContainerResource} to run a Historical node.
   */
  public static DruidContainerResource newHistorical()
  {
    return new DruidContainerResource(DruidCommand.HISTORICAL)
        .addProperty("druid.processing.buffer.sizeBytes", "50MiB")
        .addProperty("druid.processing.numMergeBuffers", "2")
        .addProperty("druid.processing.numThreads", "5");
  }

  /**
   * Creates a new {@link DruidContainerResource} to run a Broker node.
   */
  public static DruidContainerResource newBroker()
  {
    return new DruidContainerResource(DruidCommand.BROKER)
        .addProperty("druid.lookup.enableLookupSyncOnStartup", "false")
        .addProperty("druid.processing.buffer.sizeBytes", "50MiB")
        .addProperty("druid.processing.numMergeBuffers", "2")
        .addProperty("druid.processing.numThreads", "5");
  }

  /**
   * Creates a new {@link DruidContainerResource} to run a Router node.
   */
  public static DruidContainerResource newRouter()
  {
    return new DruidContainerResource(DruidCommand.ROUTER);
  }

  /**
   * Updates the given URI by replacing occurrence of {@code localhost} or
   * {@code 127.0.0.1} with {@code host.docker.internal}. This allows a service
   * running in a DruidContainer to connect to services on the host machine.
   *
   * @throws IAE if the given connectUri does not contain {@code localhost} or
   * {@code 127.0.0.1}.
   */
  public static String makeUriContainerCompatible(String connectUri, String compatibleHostname)
  {
    if (connectUri.contains("localhost")) {
      return StringUtils.replace(connectUri, "localhost", compatibleHostname);
    } else if (connectUri.contains("127.0.0.1")) {
      return StringUtils.replace(connectUri, "127.0.0.1", compatibleHostname);
    } else {
      throw new IAE(
          "Connect URL[%s] must have 'localhost' or '127.0.0.1' as host to be"
          + " reachable by DruidContainers.",
          connectUri
      );
    }
  }
}
