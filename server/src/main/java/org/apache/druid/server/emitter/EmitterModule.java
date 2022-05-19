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

package org.apache.druid.server.emitter;

import com.google.common.base.Strings;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Binding;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.server.DruidNode;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 *
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
    binder.install(new ParametrizedUriEmitterModule());
    binder.install(new ComposingEmitterModule());

    binder.bind(Emitter.class).toProvider(new EmitterProvider(emitterType)).in(LazySingleton.class);

    MapBinder<String, String> extraServiceDimensions = MapBinder.newMapBinder(
        binder,
        String.class,
        String.class,
        ExtraServiceDimensions.class
    );
    String version = getClass().getPackage().getImplementationVersion();
    extraServiceDimensions
        .addBinding("version")
        .toInstance(StringUtils.nullToEmptyNonDruidDataString(version)); // Version is null during `mvn test`.
  }

  @Provides
  @ManageLifecycle
  public ServiceEmitter getServiceEmitter(
      @Self Supplier<DruidNode> configSupplier,
      Emitter emitter,
      @ExtraServiceDimensions Map<String, String> extraServiceDimensions
  )
  {
    final DruidNode config = configSupplier.get();
    log.info("Using emitter [%s] for metrics and alerts, with dimensions [%s].", emitter, extraServiceDimensions);
    final ServiceEmitter retVal = new ServiceEmitter(
        config.getServiceName(),
        config.getHostAndPortToUse(),
        emitter,
        ImmutableMap.copyOf(extraServiceDimensions)
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
      final List<Binding<Emitter>> emitterBindings = injector.findBindingsByType(new TypeLiteral<Emitter>() {});

      if (Strings.isNullOrEmpty(emitterType)) {
        // If the emitter is unspecified, we want to default to the no-op emitter. Include empty string here too, just
        // in case nulls are translated to empty strings at some point somewhere in the system.
        emitter = findEmitter(NoopEmitterModule.EMITTER_TYPE, emitterBindings);
      } else {
        emitter = findEmitter(emitterType, emitterBindings);
      }

      if (emitter == null) {
        // If the requested emitter couldn't be found, throw an error. It might mean a typo, or a missing extension.
        List<String> knownTypes = new ArrayList<>();
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
