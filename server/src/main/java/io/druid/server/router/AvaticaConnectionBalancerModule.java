/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.server.router;

import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import io.druid.guice.LazySingleton;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Properties;

/**
 */
public class AvaticaConnectionBalancerModule implements Module
{
  private static final Logger log = new Logger(AvaticaConnectionBalancerModule.class);
  private static final String BALANCER_PROPERTY = "druid.router.avatica.balancer";

  private final Properties props;

  @Inject
  public AvaticaConnectionBalancerModule(
      Properties props
  )
  {
    this.props = props;
  }

  @Override
  public void configure(Binder binder)
  {
    String balancerType = props.getProperty(BALANCER_PROPERTY, "");
    binder.install(new ConsistentHashAvaticaConnectionBalancerModule());
    binder.install(new RendezvousHashAvaticaConnectionBalancerModule());
    binder.bind(AvaticaConnectionBalancer.class)
          .toProvider(new AvaticaConnectionBalancerProvider(balancerType))
          .in(LazySingleton.class);
  }

  private static class AvaticaConnectionBalancerProvider implements Provider<AvaticaConnectionBalancer>
  {
    private final String balancerType;

    private AvaticaConnectionBalancer balancer = null;

    AvaticaConnectionBalancerProvider(
        String balancerType
    )
    {
      this.balancerType = balancerType;
    }

    @Inject
    public void inject(Injector injector)
    {
      final List<Binding<AvaticaConnectionBalancer>> balancerBindings = injector.findBindingsByType(
          new TypeLiteral<AvaticaConnectionBalancer>(){}
      );

      balancer = findBalancer(balancerType, balancerBindings);

      if (balancer == null) {
        balancer = findBalancer(RendezvousHashAvaticaConnectionBalancerModule.BALANCER_TYPE, balancerBindings);
      }

      if (balancer == null) {
        List<String> knownTypes = Lists.newArrayList();
        for (Binding<AvaticaConnectionBalancer> binding : balancerBindings) {
          final Annotation annotation = binding.getKey().getAnnotation();
          if (annotation != null) {
            knownTypes.add(((Named) annotation).value());
          }
        }
        throw new ISE("Unknown balancer type[%s]=[%s], known types[%s]", BALANCER_PROPERTY, balancerType, knownTypes);
      }
    }

    private AvaticaConnectionBalancer findBalancer(String balancerType, List<Binding<AvaticaConnectionBalancer>> balancerBindings)
    {
      for (Binding<AvaticaConnectionBalancer> binding : balancerBindings) {
        if (Names.named(balancerType).equals(binding.getKey().getAnnotation())) {
          return binding.getProvider().get();
        }
      }
      return null;
    }


    @Override
    public AvaticaConnectionBalancer get()
    {
      if (balancer == null) {
        throw new ISE("AvaticaConnectionBalancer was null, that's bad!");
      }
      return balancer;
    }
  }
}
