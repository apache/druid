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

package io.druid.guice;

import com.google.common.collect.Iterables;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.ProvisionException;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Names;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

/**
 */
public class PolyBindTest
{
  private Properties props;
  private Injector injector;

  public void setUp(Module... modules) throws Exception
  {
    props = new Properties();
    injector = Guice.createInjector(
        Iterables.concat(
            Collections.singletonList(
                new Module()
                {
                  @Override
                  public void configure(Binder binder)
                  {
                    binder.bind(Properties.class).toInstance(props);
                    PolyBind.createChoice(binder, "billy", Key.get(Gogo.class), Key.get(GoA.class));
                    PolyBind.createChoiceWithDefault(binder, "sally", Key.get(GogoSally.class), "b");

                  }
                }
            ),
            Arrays.asList(modules)
        )
    );
  }

  @Test
  public void testSanity() throws Exception
  {
    setUp(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            final MapBinder<String, Gogo> gogoBinder = PolyBind.optionBinder(binder, Key.get(Gogo.class));
            gogoBinder.addBinding("a").to(GoA.class);
            gogoBinder.addBinding("b").to(GoB.class);

            final MapBinder<String, GogoSally> gogoSallyBinder = PolyBind.optionBinder(binder, Key.get(GogoSally.class));
            gogoSallyBinder.addBinding("a").to(GoA.class);
            gogoSallyBinder.addBinding("b").to(GoB.class);

            PolyBind.createChoice(
                binder, "billy", Key.get(Gogo.class, Names.named("reverse")), Key.get(GoB.class)
            );
            final MapBinder<String,Gogo> annotatedGogoBinder = PolyBind.optionBinder(
                binder, Key.get(Gogo.class, Names.named("reverse"))
            );
            annotatedGogoBinder.addBinding("a").to(GoB.class);
            annotatedGogoBinder.addBinding("b").to(GoA.class);
          }
        }
    );


    Assert.assertEquals("A", injector.getInstance(Gogo.class).go());
    Assert.assertEquals("B", injector.getInstance(Key.get(Gogo.class, Names.named("reverse"))).go());
    props.setProperty("billy", "b");
    Assert.assertEquals("B", injector.getInstance(Gogo.class).go());
    Assert.assertEquals("A", injector.getInstance(Key.get(Gogo.class, Names.named("reverse"))).go());
    props.setProperty("billy", "a");
    Assert.assertEquals("A", injector.getInstance(Gogo.class).go());
    Assert.assertEquals("B", injector.getInstance(Key.get(Gogo.class, Names.named("reverse"))).go());
    props.setProperty("billy", "b");
    Assert.assertEquals("B", injector.getInstance(Gogo.class).go());
    Assert.assertEquals("A", injector.getInstance(Key.get(Gogo.class, Names.named("reverse"))).go());
    props.setProperty("billy", "c");
    try {
      Assert.assertEquals("A", injector.getInstance(Gogo.class).go());
      Assert.fail(); // should never be reached
    }
    catch (Exception e) {
      Assert.assertTrue(e instanceof ProvisionException);
      Assert.assertTrue(e.getMessage().contains("Unknown provider[c] of Key[type=io.druid.guice.PolyBindTest$Gogo"));
    }
    try {
      Assert.assertEquals("B", injector.getInstance(Key.get(Gogo.class, Names.named("reverse"))).go());
      Assert.fail(); // should never be reached
    }
    catch (Exception e) {
      Assert.assertTrue(e instanceof ProvisionException);
      Assert.assertTrue(e.getMessage().contains("Unknown provider[c] of Key[type=io.druid.guice.PolyBindTest$Gogo"));
    }
    
    // test default property value
    Assert.assertEquals("B", injector.getInstance(GogoSally.class).go());
    props.setProperty("sally", "a");
    Assert.assertEquals("A", injector.getInstance(GogoSally.class).go());
    props.setProperty("sally", "b");
    Assert.assertEquals("B", injector.getInstance(GogoSally.class).go());
    props.setProperty("sally", "c");
    try {
      injector.getInstance(GogoSally.class).go();
      Assert.fail(); // should never be reached
    }
    catch (Exception e) {
      Assert.assertTrue(e instanceof ProvisionException);
      Assert.assertTrue(e.getMessage().contains("Unknown provider[c] of Key[type=io.druid.guice.PolyBindTest$GogoSally"));
    }
  }

  public static interface Gogo
  {
    public String go();
  }

  public static interface GogoSally
  {
    public String go();
  }

  public static class GoA implements Gogo, GogoSally
  {
    @Override
    public String go()
    {
      return "A";
    }
  }

  public static class GoB implements Gogo, GogoSally
  {
    @Override
    public String go()
    {
      return "B";
    }
  }
}
