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

  public static DruidContainerResource newCoordinator()
  {
    return new DruidContainerResource(DruidCommand.COORDINATOR);
  }

  public static DruidContainerResource newOverlord()
  {
    return new DruidContainerResource(DruidCommand.OVERLORD);
  }

  public static DruidContainerResource newMiddleManager()
  {
    return new DruidContainerResource(DruidCommand.MIDDLE_MANAGER);
  }

  public static DruidContainerResource newHistorical()
  {
    return new DruidContainerResource(DruidCommand.HISTORICAL);
  }

  public static DruidContainerResource newBroker()
  {
    return new DruidContainerResource(DruidCommand.BROKER);
  }
}
