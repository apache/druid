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

package org.apache.druid.initialization;

import com.fasterxml.jackson.databind.Module;
import org.apache.druid.guice.annotations.ExtensionPoint;

import java.util.List;

/**
 * A Guice module which also provides Jackson modules.
 * There is generally no need to implement this interface unless
 * you want to define Jackson modules: use the Guice
 * {@code Module} otherwise.
 * <p>
 * Extension modules extend this interface rather than from
 * the Guice {@code Module} interface.
 */
@ExtensionPoint
public interface DruidModule extends com.google.inject.Module
{
  List<? extends Module> getJacksonModules();
}
