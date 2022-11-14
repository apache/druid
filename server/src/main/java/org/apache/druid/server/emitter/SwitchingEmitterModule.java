package org.apache.druid.server.emitter;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.SwitchingEmitter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class SwitchingEmitterModule implements Module
{

  public static final String EMITTER_TYPE = "switching";

  private static Logger log = new Logger(SwitchingEmitterModule.class);

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter.switching", SwitchingEmitterConfig.class);
  }

  @Provides
  @ManageLifecycle
  @Named(EMITTER_TYPE)
  public Emitter makeEmitter(SwitchingEmitterConfig config, final Injector injector)
  {
    log.info(
        "Crfeateing Switching emitter with %s, and default emitter %s",
        config.getEmitters(),
        config.getDefaultEmitter()
    );
    Map<String, List<Emitter>> switchingEmitters = config
        .getEmitters()
        .entrySet()
        .stream()
        .collect(
            Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue()
                              .stream()
                              .map(
                                  emitterName -> injector.getInstance(Key.get(Emitter.class, Names.named(emitterName))))
                              .collect(Collectors.toList())));

    ImmutableList.Builder<Emitter> defaultEmittersBuilder = new ImmutableList.Builder<>();
    for (String emitterName : config.getDefaultEmitter()) {
      defaultEmittersBuilder.add(injector.getInstance(Key.get(Emitter.class, Names.named(emitterName))));
    }
    return new SwitchingEmitter(switchingEmitters, defaultEmittersBuilder.build().toArray(new Emitter[0]));
  }
}
