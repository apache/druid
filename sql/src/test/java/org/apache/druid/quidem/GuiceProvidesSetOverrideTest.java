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

package org.apache.druid.quidem;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.multibindings.ProvidesIntoSet;
import com.google.inject.util.Modules;
import org.junit.jupiter.api.Test;

import java.util.Properties;
import java.util.Set;

public class GuiceProvidesSetOverrideTest
{

  static class U
  {
    private Set<Properties> props;

    @Inject
    public U(Set<Properties> props)
    {
      this.props = props;
    }

    public void print()
    {
      for (Properties properties : props) {
        System.out.println(properties);
      }

    }
  }

  public class M1 implements com.google.inject.Module
  {

    private String string;

    public M1(String string)
    {
      this.string = string;

    }

    @ProvidesIntoSet
    public Properties provideProperties()
    {
      Properties p = new Properties();
      p.put("key", "value" + string);
      return p;
    }

    @ProvidesIntoSet
    public Properties provideProperties2()
    {
      Properties p = new Properties();
      p.put("key2", "value2" + string);
      return p;
    }

    @Override
    public void configure(Binder binder)
    {
    }

  }

  @Test
  public void test()
  {

    Injector i = Guice.createInjector(new M1("x"));
    U u = i.getInstance(U.class);
    u.print();
  }

  @Test

  public void test2()
  {

    Injector i = Guice.createInjector(Modules.override(new M1("x")).with(new M1("y")));
    U u = i.getInstance(U.class);
    u.print();
  }

}
