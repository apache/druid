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

import org.apache.druid.error.DruidException;
import org.testcontainers.containers.GenericContainer;

import javax.annotation.Nullable;

/**
 * Abstract base class for resources that use Docker containers from Testcontainers.
 */
public abstract class TestcontainerResource<T extends GenericContainer<?>> implements EmbeddedResource
{
  @Nullable
  private T container;

  /**
   * Creates the container instance. This method is called by {@link #start()} to create
   * a new container. Subclasses should configure the container with appropriate image,
   * ports, and other settings.
   */
  protected abstract T createContainer();

  @Override
  public void start()
  {
    container = createContainer();
    container.start();
  }

  @Override
  public void stop()
  {
    if (container != null) {
      container.stop();
      container = null;
    }
  }

  /**
   * Returns whether the container is currently running.
   */
  public boolean isRunning()
  {
    return container != null && container.isRunning();
  }

  /**
   * Returns the host address of the running container.
   */
  public String getHost()
  {
    ensureRunning();
    return container.getHost();
  }

  /**
   * Returns the mapped port for a given exposed port.
   */
  public int getMappedPort(int exposedPort)
  {
    ensureRunning();
    return container.getMappedPort(exposedPort);
  }

  /**
   * Returns the underlying container instance.
   */
  @Nullable
  public T getContainer()
  {
    return container;
  }

  protected void ensureRunning()
  {
    if (!isRunning()) {
      throw DruidException.defensive("Container is not running");
    }
  }
}
