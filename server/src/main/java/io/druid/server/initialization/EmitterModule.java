/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.server.initialization;

import com.google.common.base.Supplier;
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
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.emitter.EmittingLogger;
import com.metamx.emitter.core.Emitter;
import com.metamx.emitter.service.ServiceEmitter;
import io.druid.guice.LazySingleton;
import io.druid.guice.ManageLifecycle;
import io.druid.guice.annotations.Self;
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

    binder.install(new LogEmitterModule());
    binder.install(new HttpEmitterModule());

    binder.bind(Emitter.class).toProvider(new EmitterProvider(emitterType)).in(LazySingleton.class);
  }

  @Provides
  @ManageLifecycle
  public ServiceEmitter getServiceEmitter(@Self Supplier<DruidNode> configSupplier, Emitter emitter)
  {
    final DruidNode config = configSupplier.get();
    final ServiceEmitter retVal = new ServiceEmitter(config.getServiceName(), config.getHost(), emitter);
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
        emitter = findEmitter(LogEmitterModule.EMITTER_TYPE, emitterBindings);
      }

      if (emitter == null) {
        List<String> knownTypes = Lists.newArrayList();
        for (Binding<Emitter> binding : emitterBindings) {
          final Annotation annotation = binding.getKey().getAnnotation();
          if (annotation != null) {
            knownTypes.add(((Named) annotation).value());
          }
        }
        throw new ISE("Uknown emitter type[%s]=[%s], known types[%s]", EMITTER_PROPERTY, emitterType, knownTypes);
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
