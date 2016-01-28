/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.coordinator.helper;

import io.druid.server.coordinator.DruidCoordinatorRuntimeParams;

/**
 */
public interface DruidCoordinatorHelper
{
  /**
   * Implementations of this method run various activities performed by the coordinator.
   * Input params can be used and modified. They are typically in a list and returned
   * DruidCoordinatorRuntimeParams is passed to the next helper.
   * @param params
   * @return same as input or a modified value to be used by next helper.
   */
  DruidCoordinatorRuntimeParams run(DruidCoordinatorRuntimeParams params);
}
