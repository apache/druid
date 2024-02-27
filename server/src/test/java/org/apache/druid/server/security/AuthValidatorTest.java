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

import org.apache.druid.error.DruidExceptionMatcher;
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
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [authorizerName]: must not be null"
    ).assertThrowsAndMatches(() -> target.validateAuthorizerName(""));
  }

  @Test
  public void testAuthorizerNameWithNullIsInvalid()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [authorizerName]: must not be null"
    ).assertThrowsAndMatches(() -> target.validateAuthorizerName(null));
  }

  @Test
  public void testAuthorizerNameStartsWithDotIsInValid()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [authorizerName]: Value [.test] cannot start with '.'."
    ).assertThrowsAndMatches(() -> target.validateAuthorizerName(".test"));
  }

  @Test
  public void testAuthorizerNameWithSlashIsInvalid()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [authorizerName]: Value [tes/t] cannot contain '/'."
    ).assertThrowsAndMatches(() -> target.validateAuthorizerName("tes/t"));
  }

  @Test
  public void testAuthorizerNameWithWhitespaceIsInvalid()
  {
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [authorizerName]: Value [tes\tt] contains illegal whitespace characters.  Only space is allowed."
    ).assertThrowsAndMatches(() -> target.validateAuthorizerName("tes\tt"));
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
    DruidExceptionMatcher.invalidInput().expectMessageIs(
        "Invalid value for field [authenticatorName]: Value [tes\tt] contains illegal whitespace characters.  Only space is allowed."
    ).assertThrowsAndMatches(() -> target.validateAuthenticatorName("tes\tt"));
  }
}
