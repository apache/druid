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

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.metamx.common.lifecycle.Lifecycle;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import junit.framework.Assert;
import org.junit.Test;

/**
 */
public class LifecycleScopeTest
{
  @Test
  public void testAnnotation() throws Exception
  {
    final Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(TestInterface.class).to(AnnotatedClass.class);
          }
        }
    );

    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

    final TestInterface instance = injector.getInstance(TestInterface.class);

    testIt(injector, lifecycle, instance);
  }

  @Test
  public void testExplicit() throws Exception
  {
    final Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(TestInterface.class).to(ExplicitClass.class).in(ManageLifecycle.class);
          }
        }
    );

    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

    final TestInterface instance = injector.getInstance(TestInterface.class);

    testIt(injector, lifecycle, instance);
  }

  private void testIt(Injector injector, Lifecycle lifecycle, TestInterface instance)
      throws Exception
  {
    Assert.assertEquals(0, instance.getStarted());
    Assert.assertEquals(0, instance.getStopped());
    Assert.assertEquals(0, instance.getRan());

    instance.run();
    Assert.assertEquals(0, instance.getStarted());
    Assert.assertEquals(0, instance.getStopped());
    Assert.assertEquals(1, instance.getRan());

    lifecycle.start();
    Assert.assertEquals(1, instance.getStarted());
    Assert.assertEquals(0, instance.getStopped());
    Assert.assertEquals(1, instance.getRan());

    injector.getInstance(TestInterface.class).run();  // It's a singleton
    Assert.assertEquals(1, instance.getStarted());
    Assert.assertEquals(0, instance.getStopped());
    Assert.assertEquals(2, instance.getRan());

    lifecycle.stop();
    Assert.assertEquals(1, instance.getStarted());
    Assert.assertEquals(1, instance.getStopped());
    Assert.assertEquals(2, instance.getRan());
  }

  /**
   * This is a test for documentation purposes.  It's there to show what weird things Guice will do when
   * it sees both the annotation and an explicit binding.
   *
   * @throws Exception
   */
  @Test
  public void testAnnotatedAndExplicit() throws Exception
  {
    final Injector injector = Guice.createInjector(
        new DruidGuiceExtensions(),
        new LifecycleModule(),
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.bind(TestInterface.class).to(AnnotatedClass.class).in(ManageLifecycle.class);
          }
        }
    );

    final Lifecycle lifecycle = injector.getInstance(Lifecycle.class);

    final TestInterface instance = injector.getInstance(TestInterface.class);

    Assert.assertEquals(0, instance.getStarted());
    Assert.assertEquals(0, instance.getStopped());
    Assert.assertEquals(0, instance.getRan());

    instance.run();
    Assert.assertEquals(0, instance.getStarted());
    Assert.assertEquals(0, instance.getStopped());
    Assert.assertEquals(1, instance.getRan());

    lifecycle.start();
    Assert.assertEquals(2, instance.getStarted());
    Assert.assertEquals(0, instance.getStopped());
    Assert.assertEquals(1, instance.getRan());

    injector.getInstance(TestInterface.class).run();  // It's a singleton
    Assert.assertEquals(2, instance.getStarted());
    Assert.assertEquals(0, instance.getStopped());
    Assert.assertEquals(2, instance.getRan());

    lifecycle.stop();
    Assert.assertEquals(2, instance.getStarted());
    Assert.assertEquals(2, instance.getStopped());
    Assert.assertEquals(2, instance.getRan());
  }

  private static interface TestInterface
  {
    public void run();
    public int getStarted();
    public int getStopped();
    public int getRan();
  }

  @ManageLifecycle
  public static class AnnotatedClass implements TestInterface
  {
    int started = 0;
    int stopped = 0;
    int ran = 0;

    @LifecycleStart
    public void start()
    {
      ++started;
    }

    @LifecycleStop
    public void stop()
    {
      ++stopped;
    }

    @Override
    public void run()
    {
      ++ran;
    }

    public int getStarted()
    {
      return started;
    }

    public int getStopped()
    {
      return stopped;
    }

    public int getRan()
    {
      return ran;
    }
  }

  public static class ExplicitClass implements TestInterface
  {
    int started = 0;
    int stopped = 0;
    int ran = 0;

    @LifecycleStart
    public void start()
    {
      ++started;
    }

    @LifecycleStop
    public void stop()
    {
      ++stopped;
    }

    @Override
    public void run()
    {
      ++ran;
    }

    public int getStarted()
    {
      return started;
    }

    public int getStopped()
    {
      return stopped;
    }

    public int getRan()
    {
      return ran;
    }
  }

}
