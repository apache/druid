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

package org.apache.druid.java.util.emitter.core;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SwitchingEmitterTest
{

  private static final String FEED_1 = "feed1";
  private static final String FEED_2 = "feed2";
  private static final String FEED_3 = "feed3";
  private SwitchingEmitter switchingEmitter;

  private Map<String, List<Emitter>> emitters;
  private List<Emitter> defaultEmitters;

  private Emitter feed1Emitter1;
  private Emitter feed1Emitter2;
  private Emitter feed2Emitter1;
  private Emitter feed1AndFeed3Emitter;

  private Set<Emitter> allEmitters;

  @Before
  public void setup()
  {
    this.defaultEmitters = ImmutableList.of(
        EasyMock.createMock(Emitter.class),
        EasyMock.createMock(Emitter.class)
    );
    this.feed1Emitter1 = EasyMock.createMock(Emitter.class);
    this.feed1Emitter2 = EasyMock.createMock(Emitter.class);
    this.feed2Emitter1 = EasyMock.createMock(Emitter.class);
    this.feed1AndFeed3Emitter = EasyMock.createMock(Emitter.class);
    this.emitters = ImmutableMap.of(FEED_1, ImmutableList.of(feed1Emitter1, feed1Emitter2, feed1AndFeed3Emitter),
                                    FEED_2, ImmutableList.of(feed2Emitter1),
                                    FEED_3, ImmutableList.of(feed1AndFeed3Emitter));

    allEmitters = new HashSet<>();
    allEmitters.addAll(defaultEmitters);
    for (List<Emitter> feedEmitters : emitters.values()) {
      allEmitters.addAll(feedEmitters);
    }
    this.switchingEmitter = new SwitchingEmitter(emitters, defaultEmitters.toArray(new Emitter[0]));
  }

  @Test
  public void testStart()
  {
    for (Emitter emitter : allEmitters) {
      emitter.start();
      EasyMock.replay(emitter);
    }

    switchingEmitter.start();
  }

  @Test
  public void testEmit()
  {
    // test emitting events to all 3 feeds and default emitter
    Event feed1Event = EasyMock.createMock(Event.class);
    Event feed2Event = EasyMock.createMock(Event.class);
    Event feed3Event = EasyMock.createMock(Event.class);
    Event eventWithNoMatchingFeed = EasyMock.createMock(Event.class);

    EasyMock.expect(feed1Event.getFeed()).andReturn(FEED_1).anyTimes();
    EasyMock.expect(feed2Event.getFeed()).andReturn(FEED_2).anyTimes();
    EasyMock.expect(feed3Event.getFeed()).andReturn(FEED_3).anyTimes();
    EasyMock.expect(eventWithNoMatchingFeed.getFeed()).andReturn("no-real-feed").anyTimes();
    EasyMock.replay(feed1Event, feed2Event, feed3Event, eventWithNoMatchingFeed);

    for (Emitter emitter : defaultEmitters) {
      emitter.emit(eventWithNoMatchingFeed);
    }
    for (Emitter emitter : emitters.get("feed1")) {
      emitter.emit(feed1Event);
    }
    for (Emitter emitter : emitters.get("feed2")) {
      emitter.emit(feed2Event);
    }
    for (Emitter emitter : emitters.get("feed3")) {
      emitter.emit(feed3Event);
    }
    for (Emitter emitter : allEmitters) {
      EasyMock.replay(emitter);
    }

    switchingEmitter.emit(feed1Event);
    switchingEmitter.emit(feed2Event);
    switchingEmitter.emit(feed3Event);
    switchingEmitter.emit(eventWithNoMatchingFeed);
  }

  @Test
  public void testFlush() throws IOException
  {
    for (Emitter emitter : allEmitters) {
      emitter.flush();
      EasyMock.replay(emitter);
    }

    switchingEmitter.flush();
  }

  @Test
  public void testClose() throws IOException
  {
    for (Emitter emitter : allEmitters) {
      emitter.close();
      EasyMock.replay(emitter);
    }

    switchingEmitter.close();
  }

  @After
  public void tearDown()
  {
    for (Emitter emitter : allEmitters) {
      EasyMock.verify(emitter);
    }
  }
}
