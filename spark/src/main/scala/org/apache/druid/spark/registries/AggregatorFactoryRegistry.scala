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
import org.apache.druid.query.aggregation.AggregatorFactory
import org.apache.druid.spark.MAPPER

/**
  * A registry for aggregator factories. Since this is only to support deserialization when
  * constructing an AggregatorFactory[] in DruidDataWriterFactory, we can shadow the usual Druid
  * pattern and let Jackson handle the polymorphism. If we ever need to instantiate
  * AggregatorFactories ourselves, this will have to be changed.
  */
object AggregatorFactoryRegistry {
  /**
    * Register an aggregator factory with the given name. NAME must match the Jackson sub-type for AGGREGATORFACTORY.
    *
    * @param name The Jackson subtype for AGGREGATORFACTORY
    * @param factory An implementation of AggregatorFactory to use when processing metrics.
    */
  def register(name: String, factory: AggregatorFactory): Unit = {
    // Cheat
    MAPPER.registerSubtypes(new NamedType(factory.getClass, name))
  }
}
