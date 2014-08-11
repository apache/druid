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

package io.druid.examples;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.druid.examples.rand.RandomFirehoseFactory;
import io.druid.examples.twitter.TwitterSpritzerFirehoseFactory;
import io.druid.examples.web.WebFirehoseFactory;
import io.druid.initialization.DruidModule;

import java.util.Arrays;
import java.util.List;

/**
 */
public class ExamplesDruidModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("ExamplesModule")
            .registerSubtypes(
                new NamedType(TwitterSpritzerFirehoseFactory.class, "twitzer"),
                new NamedType(RandomFirehoseFactory.class, "rand"),
                new NamedType(WebFirehoseFactory.class, "webstream")
            )
    );
  }

  @Override
  public void configure(Binder binder)
  {

  }
}
