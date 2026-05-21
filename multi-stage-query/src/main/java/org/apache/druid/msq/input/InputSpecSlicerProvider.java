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

package org.apache.druid.msq.input;

import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.query.QueryContext;

import java.util.List;

/**
 * Controller-side extension point for {@link InputSpec}: provides an {@link InputSpecSlicer} for a particular
 * {@link InputSpec} class.
 */
public interface InputSpecSlicerProvider
{
  /**
   * The {@link InputSpec} class handled by this provider.
   */
  Class<? extends InputSpec> specClass();

  /**
   * Returns an {@link InputSpecSlicer} that accepts {@link #specClass()}.
   */
  InputSpecSlicer createSlicer(
      ControllerContext controllerContext,
      QueryContext queryContext,
      List<String> workerIds
  );
}
