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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import druid.examples.flights.FlightsFirehoseFactory;
import druid.examples.rand.RandomFirehoseFactory;
import druid.examples.twitter.TwitterSpritzerFirehoseFactory;
import druid.examples.web.WebFirehoseFactory;
import io.druid.indexing.common.index.EventReceiverFirehoseFactory;
import io.druid.segment.realtime.firehose.LocalFirehoseFactory;
import io.druid.indexing.common.index.StaticS3FirehoseFactory;
import io.druid.initialization.DruidModule;
import io.druid.segment.realtime.firehose.ClippedFirehoseFactory;
import io.druid.segment.realtime.firehose.IrcFirehoseFactory;
import io.druid.segment.realtime.firehose.KafkaFirehoseFactory;
import io.druid.segment.realtime.firehose.RabbitMQFirehoseFactory;
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
                new NamedType(TwitterSpritzerFirehoseFactory.class, "twitzer"),
                new NamedType(FlightsFirehoseFactory.class, "flights"),
                new NamedType(RandomFirehoseFactory.class, "rand"),
                new NamedType(WebFirehoseFactory.class, "webstream"),
                new NamedType(KafkaFirehoseFactory.class, "kafka-0.7.2"),
                new NamedType(RabbitMQFirehoseFactory.class, "rabbitmq"),
                new NamedType(ClippedFirehoseFactory.class, "clipped"),
                new NamedType(TimedShutoffFirehoseFactory.class, "timed"),
                new NamedType(IrcFirehoseFactory.class, "irc"),
                new NamedType(StaticS3FirehoseFactory.class, "s3"),
                new NamedType(EventReceiverFirehoseFactory.class, "receiver"),
                new NamedType(LocalFirehoseFactory.class, "local")
            )
    );
  }
}
