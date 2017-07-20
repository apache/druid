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

package io.druid.server.initialization;

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.LazySingleton;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;

import java.lang.annotation.Annotation;
import java.util.List;
import java.util.Properties;

/**
 */
public class EmitterModule implements Module
{
  private static final Logger log = new Logger(EmitterModule.class);
  private static final String EMITTER_PROPERTY = "druid.emitter";

  private final Properties props;

  @Inject
  public EmitterModule(
      Properties props
  )
  {
    this.props = props;
  }

  @Override
  public void configure(Binder binder)
  {
    String emitterType = props.getProperty(EMITTER_PROPERTY, "");

    binder.install(new NoopEmitterModule());
    binder.install(new LogEmitterModule());
    binder.install(new HttpEmitterModule());
    binder.install(new ComposingEmitterModule());

    binder.bind(Emitter.class).toProvider(new EmitterProvider(emitterType)).in(LazySingleton.class);
  }

  @Provides
  @ManageLifecycle
  public ServiceEmitter getServiceEmitter(@Self Supplier<DruidNode> configSupplier, Emitter emitter)
  {
    final DruidNode config = configSupplier.get();
    String version = getClass().getPackage().getImplementationVersion();
    final ImmutableMap<String, String> otherServiceDimensions = ImmutableMap.of(
        "version",
        Strings.nullToEmpty(version) // Version is null during `mvn test`.
    );
    final ServiceEmitter retVal = new ServiceEmitter(
        config.getServiceName(),
        config.getHostAndPortToUse(),
        emitter,
        otherServiceDimensions
    );
    EmittingLogger.registerEmitter(retVal);
    return retVal;
  }

  private static class EmitterProvider implements Provider<Emitter>
  {
    private final String emitterType;

    private Emitter emitter = null;

    EmitterProvider(
        String emitterType
    )
    {
      this.emitterType = emitterType;
    }

    @Inject
    public void inject(Injector injector)
    {
      final List<Binding<Emitter>> emitterBindings = injector.findBindingsByType(new TypeLiteral<Emitter>(){});

      emitter = findEmitter(emitterType, emitterBindings);

      if (emitter == null) {
        emitter = findEmitter(NoopEmitterModule.EMITTER_TYPE, emitterBindings);
      }

      if (emitter == null) {
        List<String> knownTypes = Lists.newArrayList();
        for (Binding<Emitter> binding : emitterBindings) {
          final Annotation annotation = binding.getKey().getAnnotation();
          if (annotation != null) {
            knownTypes.add(((Named) annotation).value());
          }
        }
        throw new ISE("Unknown emitter type[%s]=[%s], known types[%s]", EMITTER_PROPERTY, emitterType, knownTypes);
      }
    }

    private Emitter findEmitter(String emitterType, List<Binding<Emitter>> emitterBindings)
    {
      for (Binding<Emitter> binding : emitterBindings) {
        if (Names.named(emitterType).equals(binding.getKey().getAnnotation())) {
          return binding.getProvider().get();
        }
      }
      return null;
    }


    @Override
    public Emitter get()
    {
      if (emitter == null) {
        throw new ISE("Emitter was null, that's bad!");
      }
      return emitter;
    }
  }
}
