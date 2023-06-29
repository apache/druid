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

package org.apache.druid.cli;

import com.github.rvesse.airline.builder.CliBuilder;
import org.apache.druid.guice.annotations.ExtensionPoint;

/**
 * An extension point to create a custom Druid service. Druid can understand and execute custom commands
 * to run services loaded via Druid's extension system (see {@code Initialization#getFromExtensions}). See
 * the {@code Main} class for details of groups and commands.
 *
 * Implementations should be registered in the {@code META-INF/services/org.apache.druid.cli.CliCommandCreator} file.
 */
@ExtensionPoint
public interface CliCommandCreator
{
  void addCommands(CliBuilder builder);
}
