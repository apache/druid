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
 * Production implementation of {@link DartControllerRegistry}.
 */
public class DartControllerRegistryImpl implements DartControllerRegistry
{
  private final ConcurrentHashMap<String, ControllerHolder> controllerMap = new ConcurrentHashMap<>();

  @Override
  public void register(ControllerHolder holder)
  {
    if (controllerMap.putIfAbsent(holder.getController().queryId(), holder) != null) {
      throw DruidException.defensive("Controller[%s] already registered", holder.getController().queryId());
    }
  }

  @Override
  public void remove(ControllerHolder holder)
  {
    // Remove only if the current mapping for the queryId is this specific controller.
    controllerMap.remove(holder.getController().queryId(), holder);
  }

  @Nullable
  @Override
  public Controller get(final String queryId)
  {
    final ControllerHolder holder = controllerMap.get(queryId);
    if (holder != null) {
      return holder.getController();
    } else {
      return null;
    }
  }

  @Override
  public Collection<ControllerHolder> getAllHolders()
  {
    return controllerMap.values();
  }
}
