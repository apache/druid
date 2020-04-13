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

package org.apache.druid.testing.guice;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.name.Names;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.server.security.TLSCertificateChecker;
import org.apache.druid.testing.utils.ITTLSCertificateChecker;

import java.util.Collections;
import java.util.List;

public class ITTLSCertificateCheckerModule implements DruidModule
{
  private final ITTLSCertificateChecker INSTANCE = new ITTLSCertificateChecker();

  public static final String IT_CHECKER_TYPE = "integration-test";

  @Override
  public void configure(Binder binder)
  {
    binder.bind(TLSCertificateChecker.class)
          .annotatedWith(Names.named(IT_CHECKER_TYPE))
          .toInstance(INSTANCE);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.EMPTY_LIST;
  }
}

