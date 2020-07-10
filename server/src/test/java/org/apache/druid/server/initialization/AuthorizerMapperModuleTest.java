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

package org.apache.druid.server.initialization;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.server.security.AuthValidator;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.validation.Validation;
import javax.validation.Validator;

public class AuthorizerMapperModuleTest
{
  private AuthorizerMapperModule target;
  private Injector injector;

  @Before
  public void setUp()
  {
    target = new AuthorizerMapperModule();
    injector = Guice.createInjector(
        (binder) -> {
          binder.bind(Validator.class).toInstance(Validation.buildDefaultValidatorFactory().getValidator());
          binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
        },
        target
    );
  }

  @Test
  public void testAuthorizerNameValidatorIsInjectedAsSingleton()
  {
    AuthValidator authValidator =
        injector.getInstance(AuthValidator.class);
    AuthValidator other =
        injector.getInstance(AuthValidator.class);
    Assert.assertSame(authValidator, other);
  }
}
