package io.druid.guice;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provider;
import org.junit.Assert;
import org.junit.Test;

public class GuiceInjectorsTest
{
  @Test public void testSanity()
  {
    Injector stageOne = GuiceInjectors.makeStartupInjector();

    Module module = stageOne.getInstance(DruidSecondaryModule.class);

    Injector stageTwo = Guice.createInjector(module, new Module()
    {
      @Override public void configure(Binder binder)
      {
        binder.bind(String.class).toInstance("Expected String");
        JsonConfigProvider.bind(binder, "druid.emitter.", Emitter.class);
        binder.bind(CustomEmitter.class).toProvider(new CustomEmitterFactory());
      }
    });


    CustomEmitter customEmitter = stageTwo.getInstance(CustomEmitter.class);

    Assert.assertEquals("Expected String", customEmitter.getOtherValue());
  }

  private static class Emitter {

    @JacksonInject
    private String value;

    public String getValue()
    {
      return value;
    }
  }

  private class CustomEmitterFactory implements Provider<CustomEmitter> {

    private Emitter emitter;
    private Injector injector;

    @com.google.inject.Inject
    public void configure(Injector injector) {
      this.injector = injector;
      emitter = injector.getInstance(Emitter.class);
    }

    @Override public CustomEmitter get()
    {
      return new CustomEmitter(emitter);
    }
  }

  private static class CustomEmitter
  {
    public String getOtherValue()
    {
      return emitter.getValue();
    }

    private Emitter emitter;

    public CustomEmitter(Emitter emitter){
      this.emitter = emitter;
    }
  }
}
