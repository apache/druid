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

import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.util.List;
import java.util.Properties;

public class TLSCertificateCheckerModule implements Module
{
  private static final String CHECKER_TYPE_PROPERTY = "druid.tls.certificateChecker";

  private final Properties props;

  @Inject
  public TLSCertificateCheckerModule(
      Properties props
  )
  {
    this.props = props;
  }

  @Override
  public void configure(Binder binder)
  {
    String checkerType = props.getProperty(CHECKER_TYPE_PROPERTY, DefaultTLSCertificateCheckerModule.DEFAULT_CHECKER_TYPE);

    binder.install(new DefaultTLSCertificateCheckerModule());

    binder.bind(TLSCertificateChecker.class)
          .toProvider(new TLSCertificateCheckerProvider(checkerType))
          .in(LazySingleton.class);
  }

  public static class TLSCertificateCheckerProvider implements Provider<TLSCertificateChecker>
  {
    private final String checkerType;

    private TLSCertificateChecker checker = null;

    public TLSCertificateCheckerProvider(
        String checkerType
    )
    {
      this.checkerType = checkerType;
    }

    @Inject
    public void inject(Injector injector)
    {
      final List<Binding<TLSCertificateChecker>> checkerBindings = injector.findBindingsByType(new TypeLiteral<TLSCertificateChecker>(){});

      checker = findChecker(checkerType, checkerBindings);
      if (checker == null) {
        throw new IAE("Could not find certificate checker with type: " + checkerType);
      }
    }

    @Override
    public TLSCertificateChecker get()
    {
      if (checker == null) {
        throw new ISE("Checker was null, that's bad!");
      }
      return checker;
    }

    private TLSCertificateChecker findChecker(
        String checkerType,
        List<Binding<TLSCertificateChecker>> checkerBindings
    )
    {
      for (Binding<TLSCertificateChecker> binding : checkerBindings) {
        if (Names.named(checkerType).equals(binding.getKey().getAnnotation())) {
          return binding.getProvider().get();
        }
      }
      return null;
    }
  }
}
