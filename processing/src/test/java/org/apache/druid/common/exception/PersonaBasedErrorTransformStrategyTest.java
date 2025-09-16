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

package org.apache.druid.common.exception;

import com.google.common.collect.Iterables;
import org.apache.druid.error.DruidException;
import org.apache.druid.error.DruidExceptionMatcher;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.metrics.StubServiceEmitter;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;

public class PersonaBasedErrorTransformStrategyTest
{
  private PersonaBasedErrorTransformStrategy target;
  private StubServiceEmitter emitter;

  @Before
  public void setUp() throws Exception
  {
    target = new PersonaBasedErrorTransformStrategy();
    emitter = new StubServiceEmitter();
    EmittingLogger.registerEmitter(emitter);
  }

  @Test
  public void testUserPersonaRemainsUnchanged()
  {
    DruidException druidException = DruidException.forPersona(DruidException.Persona.USER)
                                                  .ofCategory(DruidException.Category.FORBIDDEN)
                                                  .build("Permission exception");
    Assert.assertEquals(Optional.empty(), target.maybeTransform(druidException, Optional.empty()));
  }

  @Test
  public void testDeveloperPersonaIsTransformed()
  {
    DruidException druidException = DruidException.defensive().build("Test Defensive exception");

    DruidExceptionMatcher druidExceptionMatcher = new DruidExceptionMatcher(
        DruidException.Persona.USER,
        druidException.getCategory(),
        druidException.getErrorCode()
    ).expectMessageContains("Could not process the query, please contact your administrator with Error ID");

    druidExceptionMatcher.matches(target.maybeTransform(druidException, Optional.of("the-error")).get());
    Map<String, Object> alertEvent = Iterables.getOnlyElement(emitter.getAlerts()).getDataMap();
    Assert.assertEquals(DruidException.class.getName(), alertEvent.get("exceptionType"));
    Assert.assertEquals("Test Defensive exception", alertEvent.get("exceptionMessage"));
  }
}
