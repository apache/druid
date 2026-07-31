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

import org.apache.druid.msq.exec.ControllerContext;
import org.apache.druid.msq.input.InputSpec;
import org.apache.druid.msq.input.InputSpecSlicer;
import org.apache.druid.msq.input.InputSpecSlicerProvider;
import org.apache.druid.msq.input.table.TableInputSpec;
import org.apache.druid.query.QueryContext;

import java.util.List;

/**
 * Controller-side provider for {@link TableInputSpec} in Dart.
 */
public class DartTableInputSpecSlicerProvider implements InputSpecSlicerProvider
{
  @Override
  public Class<? extends InputSpec> specClass()
  {
    return TableInputSpec.class;
  }

  @Override
  public InputSpecSlicer createSlicer(
      ControllerContext controllerContext,
      QueryContext queryContext,
      List<String> workerIds
  )
  {
    return DartTableInputSpecSlicer.createFromWorkerIds(
        workerIds,
        ((DartControllerContext) controllerContext).serverView(),
        queryContext
    );
  }
}
