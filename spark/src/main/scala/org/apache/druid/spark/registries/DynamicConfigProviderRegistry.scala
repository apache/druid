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

package org.apache.druid.spark.registries

import com.fasterxml.jackson.databind.jsontype.NamedType
import org.apache.druid.metadata.DynamicConfigProvider
import org.apache.druid.spark.MAPPER
import org.apache.druid.spark.mixins.Logging

/**
  * A registry for dynamic config providers. Similarly to the {@link AggregatorFactoryRegistry}, we can shadow the usual
  * Druid pattern and let Jackson handle the polymorphism for our current use cases.
  */
object DynamicConfigProviderRegistry extends Logging {
  /**
    * Register a dynamic config provider with the given name. NAME must match the Jackson sub-type for PROVIDER.
    *
    * @param name The Jackson subtype for PROVIDER
    * @param provider An implementation of DynamicConfigProvider to use when deserializing sensitive config values.
    */
  def register(name: String, provider: DynamicConfigProvider[_]): Unit = {
    logInfo(s"Registering DynamicConfigProvider $name.")
    // Cheat
    MAPPER.registerSubtypes(new NamedType(provider.getClass, name))
  }
}
