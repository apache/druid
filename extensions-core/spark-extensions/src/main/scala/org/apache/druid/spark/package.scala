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

package org.apache.druid

import com.fasterxml.jackson.core.`type`.TypeReference
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.{InjectableValues, Module, ObjectMapper}
import org.apache.druid.jackson.DefaultObjectMapper
import org.apache.druid.math.expr.ExprMacroTable
import org.apache.druid.query.expression.{LikeExprMacro, RegexpExtractExprMacro,
  TimestampCeilExprMacro, TimestampExtractExprMacro, TimestampFloorExprMacro,
  TimestampFormatExprMacro, TimestampParseExprMacro, TimestampShiftExprMacro, TrimExprMacro}
import org.apache.druid.timeline.DataSegment
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder

import scala.collection.JavaConverters.seqAsJavaListConverter

package object spark {

  private[spark] val MAPPER: ObjectMapper = new DefaultObjectMapper()

  private val injectableValues: InjectableValues =
    new InjectableValues.Std()
      .addValue(classOf[ExprMacroTable], new ExprMacroTable(Seq(
        new LikeExprMacro(),
        new RegexpExtractExprMacro(),
        new TimestampCeilExprMacro(),
        new TimestampExtractExprMacro(),
        new TimestampFormatExprMacro(),
        new TimestampParseExprMacro(),
        new TimestampShiftExprMacro(),
        new TimestampFloorExprMacro(),
        new TrimExprMacro.BothTrimExprMacro(),
        new TrimExprMacro.LeftTrimExprMacro(),
        new TrimExprMacro.RightTrimExprMacro()).asJava))
      .addValue(classOf[ObjectMapper], MAPPER)
      .addValue(classOf[DataSegment.PruneSpecsHolder], PruneSpecsHolder.DEFAULT)

  MAPPER.setInjectableValues(injectableValues)

  def serialize(obj: AnyRef): String = {
    MAPPER.writeValueAsString(obj)
  }

  def deserialize[T](json: String, typeReference: TypeReference[T]): T = {
    MAPPER.readValue[T](json, typeReference)
  }

  def registerModules(modules: Module*): ObjectMapper = {
    MAPPER.registerModules(modules: _*)
  }

  def registerSubType(subTypeClass: Class[_], name: String): Unit = {
    MAPPER.registerSubtypes(new NamedType(subTypeClass, name))
  }
}

