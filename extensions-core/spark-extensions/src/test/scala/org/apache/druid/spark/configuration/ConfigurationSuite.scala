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

package org.apache.druid.spark.configuration

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class ConfigurationSuite extends AnyFunSuite with Matchers {
  private val testConf: Configuration = Configuration(Map[String, String](
    Configuration.toKey("test", "sub-conf", "property") -> "testProp",
    Configuration.toKey("test", "property") -> "quizProp",
    "property" -> "examProperty"
  ))

  test("isPresent should correctly handle single keys as well as paths to properties") {
    testConf.isPresent("property") should be(true)
    testConf.isPresent("test", "sub-conf", "property") should be(true)
    testConf.isPresent("test") should be(false)
    testConf.isPresent("exam", "sub-conf", "property") should be(false)
    testConf.isPresent("test.property") should be(true)
    testConf.isPresent("test.property.property") should be(false)
  }

  test("dive should correctly return sub-configurations") {
    val subConf = testConf.dive("test")
    subConf.getString("property") should equal("quizProp")
    subConf.dive("sub-conf") should equal(Configuration.fromKeyValue("property", "testProp"))
    subConf.dive("sub-conf") should equal(testConf.dive("test", "sub-conf"))
  }

  test("dive should return empty maps when called on uncontained namespaces") {
    testConf.dive("exam").toMap.isEmpty should be(true)
  }

  test("Configurations should be case-insensitive") {
    testConf.getString("pRoPeRtY") should equal(testConf.getString("property"))
  }
}
