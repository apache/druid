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

/**
 * Registry for actively-running {@link Controller}.
 */
public interface DartControllerRegistry
{
  /**
   * Add a controller. Throws {@link DruidException} if a controller with the same {@link Controller#queryId()} is
   * already registered.
   */
  void register(ControllerHolder holder);

  /**
   * Remove a controller from the registry.
   */
  void remove(ControllerHolder holder);

  /**
   * Return a specific controller holder, or null if it doesn't exist.
   */
  @Nullable
  ControllerHolder get(String queryId);

  /**
   * Returns all actively-running {@link Controller}.
   */
  Collection<ControllerHolder> getAllHolders();
}
