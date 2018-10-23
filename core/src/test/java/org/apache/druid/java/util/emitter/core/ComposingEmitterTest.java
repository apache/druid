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
import org.easymock.EasyMock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ComposingEmitterTest
{

  private List<Emitter> childEmitters;
  private ComposingEmitter composingEmitter;

  @Before
  public void setup()
  {
    this.childEmitters = ImmutableList.of(
        EasyMock.createMock(Emitter.class),
        EasyMock.createMock(Emitter.class)
    );
    this.composingEmitter = new ComposingEmitter(childEmitters);
  }

  @Test
  public void testStart()
  {
    for (Emitter emitter : childEmitters) {
      emitter.start();
      EasyMock.replay(emitter);
    }

    composingEmitter.start();
  }

  @Test
  public void testEmit()
  {
    Event e = EasyMock.createMock(Event.class);

    for (Emitter emitter : childEmitters) {
      emitter.emit(e);
      EasyMock.replay(emitter);
    }

    composingEmitter.emit(e);
  }

  @Test
  public void testFlush() throws IOException
  {
    for (Emitter emitter : childEmitters) {
      emitter.flush();
      EasyMock.replay(emitter);
    }

    composingEmitter.flush();
  }

  @Test
  public void testClose() throws IOException
  {
    for (Emitter emitter : childEmitters) {
      emitter.close();
      EasyMock.replay(emitter);
    }

    composingEmitter.close();
  }

  @After
  public void tearDown()
  {
    for (Emitter emitter : childEmitters) {
      EasyMock.verify(emitter);
    }
  }
}
