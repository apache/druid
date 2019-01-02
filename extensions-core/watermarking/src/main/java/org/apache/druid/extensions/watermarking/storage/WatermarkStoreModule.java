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

package org.apache.druid.extensions.watermarking.storage;

import com.google.inject.Binder;
import com.google.inject.Module;

public abstract class WatermarkStoreModule implements Module
{
  public static final String STORE_PROPERTY_BASE = "druid.watermarking.store.";
  public static final String SINK_PROPERTY = WatermarkSinkModule.PROPERTY;
  public static final String SOURCE_PROPERTY = WatermarkSourceModule.PROPERTY;

  final String type;

  public WatermarkStoreModule(String type)
  {
    this.type = type;
  }

  @Override
  public void configure(Binder binder)
  {
  }
}
