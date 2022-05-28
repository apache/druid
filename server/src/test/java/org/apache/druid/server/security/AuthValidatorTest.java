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

package org.apache.druid.server.security;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class AuthValidatorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  public AuthValidator target;

  @Before
  public void setUp()
  {
    target = new AuthValidator();
  }

  @Test
  public void testAuthorizerNameWithEmptyIsInvalid()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("authorizerName cannot be null or empty.");
    target.validateAuthorizerName("");
  }

  @Test
  public void testAuthorizerNameWithNullIsInvalid()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("authorizerName cannot be null or empty.");
    target.validateAuthorizerName(null);
  }

  @Test
  public void testAuthorizerNameStartsWithDotIsInValid()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("authorizerName cannot start with the '.' character.");
    target.validateAuthorizerName(".test");
  }

  @Test
  public void testAuthorizerNameWithSlashIsInvalid()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("authorizerName cannot contain the '/' character.");
    target.validateAuthorizerName("tes/t");
  }

  @Test
  public void testAuthorizerNameWithWhitespaceIsInvalid()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("authorizerName cannot contain whitespace character except space.");
    target.validateAuthorizerName("tes\tt");
  }

  @Test
  public void testAuthorizerNameWithAllowedCharactersIsValid()
  {
    target.validateAuthorizerName("t.e.$\\, Россия 한국中国!?");
  }

  @Test
  public void testAuthenticatorNameWithAllowedCharactersIsValid()
  {
    target.validateAuthenticatorName("t.e.$\\, Россия 한국中国!?");
  }

  @Test
  public void testAuthenticatorNameWithWhitespaceIsInvalid()
  {
    expectedException.expect(IllegalArgumentException.class);
    expectedException.expectMessage("authenticatorName cannot contain whitespace character except space.");
    target.validateAuthenticatorName("tes\tt");
  }
}
