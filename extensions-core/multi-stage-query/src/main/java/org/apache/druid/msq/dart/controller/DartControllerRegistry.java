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

import org.apache.druid.error.DruidException;
import org.apache.druid.msq.exec.Controller;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry for actively-running {@link Controller}.
 */
public class DartControllerRegistry
{
  private final ConcurrentHashMap<String, ControllerHolder> controllerMap = new ConcurrentHashMap<>();

  /**
   * Add a controller. Throws {@link DruidException} if a controller with the same {@link Controller#queryId()} is
   * already registered.
   */
  public void register(ControllerHolder holder)
  {
    if (controllerMap.putIfAbsent(holder.getController().queryId(), holder) != null) {
      throw DruidException.defensive("Controller[%s] already registered", holder.getController().queryId());
    }
  }

  /**
   * Remove a controller from the registry.
   */
  public void deregister(ControllerHolder holder)
  {
    // Remove only if the current mapping for the queryId is this specific controller.
    controllerMap.remove(holder.getController().queryId(), holder);
  }

  /**
   * Return a specific controller holder, or null if it doesn't exist.
   */
  @Nullable
  public ControllerHolder get(final String queryId)
  {
    return controllerMap.get(queryId);
  }

  /**
   * Returns all actively-running {@link Controller}.
   */
  public Collection<ControllerHolder> getAllHolders()
  {
    return controllerMap.values();
  }
}
