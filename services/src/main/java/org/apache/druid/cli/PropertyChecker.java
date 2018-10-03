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

import java.util.Properties;

/**
 * The PropertyChecker classes are loaded by ServiceLoader at the very start of the program and as such MUST be on the
 * initial classpath and cannot be loaded via extensions at runtime. (Or more precisely, they are ignored if present
 * in an extension at runtime, but not on the initial classpath)
 *
 * The PropertyChecker should ONLY try and set very specific properties and any class loading should be done in an
 * isolated class loader to not pollute the general class loader
 */
public interface PropertyChecker
{
  /**
   * Check the given properties to make sure any unset values are properly configured.
   * @param properties The properties to check, usually System.getProperties()
   */
  void checkProperties(Properties properties);
}
