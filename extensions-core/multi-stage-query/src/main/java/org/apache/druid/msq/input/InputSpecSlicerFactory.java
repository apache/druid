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

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import org.apache.druid.msq.input.stage.ReadablePartitions;
import org.apache.druid.msq.input.stage.StageInputSpecSlicer;

/**
 * Creates an {@link InputSpecSlicer} given a map of stage numbers to output partitions of that stage.
 *
 * In production, this is typically used to create a {@link MapInputSpecSlicer} that contains a
 * {@link StageInputSpecSlicer}. The stage slicer needs the output partitions map in order to do its job, and
 * these aren't always known until the stage has started.
 */
public interface InputSpecSlicerFactory
{
  InputSpecSlicer makeSlicer(Int2ObjectMap<ReadablePartitions> stagePartitionsMap);
}
