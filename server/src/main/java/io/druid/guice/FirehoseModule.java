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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import io.druid.initialization.DruidModule;
import io.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import io.druid.segment.realtime.firehose.CombiningFirehoseFactory;
import io.druid.segment.realtime.firehose.EventReceiverFirehoseFactory;
import io.druid.segment.realtime.firehose.FixedCountFirehoseFactory;
import io.druid.segment.realtime.firehose.HttpFirehoseFactory;
import io.druid.segment.realtime.firehose.IrcFirehoseFactory;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.segment.realtime.firehose.TimedShutoffFirehoseFactory;

import java.util.Arrays;
import java.util.List;

/**
 */
public class FirehoseModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("FirehoseModule")
            .registerSubtypes(
                new NamedType(ClippedFirehoseFactory.class, "clipped"),
                new NamedType(TimedShutoffFirehoseFactory.class, "timed"),
                new NamedType(IrcFirehoseFactory.class, "irc"),
                new NamedType(LocalFirehoseFactory.class, "local"),
                new NamedType(HttpFirehoseFactory.class, "http"),
                new NamedType(EventReceiverFirehoseFactory.class, "receiver"),
                new NamedType(CombiningFirehoseFactory.class, "combining"),
                new NamedType(FixedCountFirehoseFactory.class, "fixedCount")
            )
    );
  }
}
