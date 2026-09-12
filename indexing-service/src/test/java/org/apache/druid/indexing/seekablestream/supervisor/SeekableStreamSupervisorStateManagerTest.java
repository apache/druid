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

package org.apache.druid.indexing.seekablestream.supervisor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager.BasicState;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManagerConfig;
import org.apache.druid.indexing.seekablestream.common.StreamException;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager.SeekableStreamExceptionEvent;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager.SeekableStreamState;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SeekableStreamSupervisorStateManagerTest
{
  private SeekableStreamSupervisorStateManager stateManager;
  private SupervisorStateManagerConfig config;
  private ObjectMapper defaultMapper;

  @BeforeEach
  public void setupTest()
  {
    config = new SupervisorStateManagerConfig(10);
    stateManager = new SeekableStreamSupervisorStateManager(config, false);
    defaultMapper = new DefaultObjectMapper();
  }

  @Test
  public void testHappyPath()
  {
    Assertions.assertEquals(BasicState.PENDING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.PENDING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.SeekableStreamState.CONNECTING_TO_STREAM);
    Assertions.assertEquals(SeekableStreamState.CONNECTING_TO_STREAM, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.DISCOVERING_INITIAL_TASKS);
    Assertions.assertEquals(SeekableStreamState.DISCOVERING_INITIAL_TASKS, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.CREATING_TASKS);
    Assertions.assertEquals(SeekableStreamState.CREATING_TASKS, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.markRunFinished();
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());


    stateManager.maybeSetState(BasicState.PENDING);
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.CONNECTING_TO_STREAM);
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.DISCOVERING_INITIAL_TASKS);
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.CREATING_TASKS);
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.markRunFinished();
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());
  }

  @Test
  public void testIdlePath()
  {
    Assertions.assertEquals(BasicState.PENDING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.PENDING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.SeekableStreamState.CONNECTING_TO_STREAM);
    Assertions.assertEquals(SeekableStreamState.CONNECTING_TO_STREAM, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.DISCOVERING_INITIAL_TASKS);
    Assertions.assertEquals(SeekableStreamState.DISCOVERING_INITIAL_TASKS, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.CREATING_TASKS);
    Assertions.assertEquals(SeekableStreamState.CREATING_TASKS, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.markRunFinished();
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    // Emulates submitting Idle notice
    stateManager.maybeSetState(BasicState.IDLE);
    Assertions.assertEquals(BasicState.IDLE, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.IDLE, stateManager.getSupervisorState().getBasicState());

    // Stay in idle state when supervisor is running until or unless it is specifically set to a different state
    stateManager.markRunFinished();
    Assertions.assertEquals(BasicState.IDLE, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.IDLE, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(BasicState.RUNNING);
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());
  }

  @Test
  public void testStoppingPath()
  {
    Assertions.assertEquals(BasicState.PENDING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.PENDING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.SeekableStreamState.CONNECTING_TO_STREAM);
    Assertions.assertEquals(SeekableStreamState.CONNECTING_TO_STREAM, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.DISCOVERING_INITIAL_TASKS);
    Assertions.assertEquals(SeekableStreamState.DISCOVERING_INITIAL_TASKS, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    // Emulates graceful shutdown
    stateManager.maybeSetState(BasicState.STOPPING);

    stateManager.maybeSetState(SeekableStreamState.CREATING_TASKS);
    Assertions.assertEquals(BasicState.STOPPING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.STOPPING, stateManager.getSupervisorState().getBasicState());

    stateManager.markRunFinished();
    Assertions.assertEquals(BasicState.STOPPING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.STOPPING, stateManager.getSupervisorState().getBasicState());
  }

  @Test
  public void testStreamFailureLostContact()
  {
    stateManager.markRunFinished(); // clean run without errors

    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());

    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      stateManager.recordThrowableEvent(new StreamException(new IllegalStateException("DOH!")));
      stateManager.markRunFinished();
    }
    Assertions.assertEquals(SeekableStreamState.LOST_CONTACT_WITH_STREAM, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState().getBasicState());
    Assertions.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());

    stateManager.getExceptionEvents().forEach(x -> {
      Assertions.assertTrue(((SeekableStreamExceptionEvent) x).isStreamException());
      Assertions.assertEquals(IllegalStateException.class.getName(), x.getExceptionClass());
    });
  }

  @Test
  public void testStreamFailureUnableToConnect()
  {
    stateManager.maybeSetState(SeekableStreamState.CONNECTING_TO_STREAM);
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assertions.assertEquals(SeekableStreamState.CONNECTING_TO_STREAM, stateManager.getSupervisorState());
      stateManager.recordThrowableEvent(new StreamException(new IllegalStateException("DOH!")));
      stateManager.markRunFinished();
    }
    Assertions.assertEquals(SeekableStreamState.UNABLE_TO_CONNECT_TO_STREAM, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState().getBasicState());
    Assertions.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());

    stateManager.getExceptionEvents().forEach(x -> {
      Assertions.assertTrue(((SeekableStreamExceptionEvent) x).isStreamException());
      Assertions.assertEquals(IllegalStateException.class.getName(), x.getExceptionClass());
    });
  }

  @Test
  public void testNonStreamUnhealthiness()
  {
    stateManager.maybeSetState(SeekableStreamState.DISCOVERING_INITIAL_TASKS);
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assertions.assertEquals(SeekableStreamState.DISCOVERING_INITIAL_TASKS, stateManager.getSupervisorState());
      stateManager.recordThrowableEvent(new NullPointerException("oof"));
      stateManager.markRunFinished();
    }
    Assertions.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState().getBasicState());
    Assertions.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());

    stateManager.getExceptionEvents().forEach(x -> {
      Assertions.assertFalse(((SeekableStreamExceptionEvent) x).isStreamException());
      Assertions.assertEquals(NullPointerException.class.getName(), x.getExceptionClass());
    });
  }

  @Test
  public void testTransientUnhealthiness()
  {
    stateManager.markRunFinished();
    for (int j = 1; j < 3; j++) {
      for (int i = 0; i < config.getUnhealthinessThreshold() - 1; i++) {
        stateManager.recordThrowableEvent(new NullPointerException("oof"));
        stateManager.markRunFinished();
        Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      }

      stateManager.markRunFinished(); // clean run
      Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());
      Assertions.assertEquals((long) j * (config.getUnhealthinessThreshold() - 1), stateManager.getExceptionEvents().size());
    }
  }

  @Test
  public void testNonTransientTaskUnhealthiness()
  {
    stateManager.markRunFinished();
    for (int i = 0; i < config.getTaskUnhealthinessThreshold(); i++) {
      Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      stateManager.recordCompletedTaskState(TaskState.FAILED);
      stateManager.markRunFinished();
    }
    Assertions.assertEquals(BasicState.UNHEALTHY_TASKS, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.UNHEALTHY_TASKS, stateManager.getSupervisorState().getBasicState());
    Assertions.assertEquals(0, stateManager.getExceptionEvents().size());
  }

  @Test
  public void testTransientTaskUnhealthiness()
  {
    // Only half are failing
    stateManager.markRunFinished();
    for (int i = 0; i < config.getTaskUnhealthinessThreshold() + 3; i++) {
      Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      stateManager.recordCompletedTaskState(TaskState.FAILED);
      stateManager.recordCompletedTaskState(TaskState.SUCCESS);
      stateManager.markRunFinished();
    }
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());
    Assertions.assertEquals(0, stateManager.getExceptionEvents().size());
  }

  @Test
  public void testSupervisorRecoveryWithHealthinessThreshold()
  {
    // Put into an unhealthy state
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assertions.assertEquals(BasicState.PENDING, stateManager.getSupervisorState());
      stateManager.recordThrowableEvent(new Exception("Except the inevitable"));
      stateManager.markRunFinished();
    }
    Assertions.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState());

    // Recover after config.healthinessThreshold successful task completions
    for (int i = 0; i < config.getHealthinessThreshold(); i++) {
      Assertions.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState());
      stateManager.markRunFinished();
    }
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assertions.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());

    stateManager.getExceptionEvents().forEach(x -> {
      Assertions.assertFalse(((SeekableStreamExceptionEvent) x).isStreamException());
      Assertions.assertEquals(Exception.class.getName(), x.getExceptionClass());
    });
  }

  @Test
  public void testTaskRecoveryWithHealthinessThreshold()
  {
    stateManager.markRunFinished();

    // Put into an unhealthy state
    for (int i = 0; i < config.getTaskUnhealthinessThreshold(); i++) {
      Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      stateManager.recordCompletedTaskState(TaskState.FAILED);
      stateManager.markRunFinished();
    }
    Assertions.assertEquals(BasicState.UNHEALTHY_TASKS, stateManager.getSupervisorState());

    // Recover after config.healthinessThreshold successful task completions
    for (int i = 0; i < config.getTaskHealthinessThreshold(); i++) {
      Assertions.assertEquals(BasicState.UNHEALTHY_TASKS, stateManager.getSupervisorState());
      stateManager.recordCompletedTaskState(TaskState.SUCCESS);
      stateManager.markRunFinished();
    }
    Assertions.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
  }

  @Test
  public void testTwoUnhealthyStates()
  {
    stateManager.markRunFinished();

    for (int i = 0; i < Math.max(config.getTaskUnhealthinessThreshold(), config.getUnhealthinessThreshold()); i++) {
      stateManager.recordThrowableEvent(new NullPointerException("somebody goofed"));
      stateManager.recordCompletedTaskState(TaskState.FAILED);
      stateManager.markRunFinished();
    }
    // UNHEALTHY_SUPERVISOR should take priority over UNHEALTHY_TASKS
    Assertions.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState());
  }

  @Test
  public void testGetThrowableEvents()
  {
    List<Exception> exceptions = ImmutableList.of(
        new StreamException(new UnsupportedOperationException("oof")),
        new NullPointerException("oof"),
        new RuntimeException(new StreamException(new Exception("oof"))),
        new RuntimeException(new IllegalArgumentException("oof"))
    );
    for (Exception exception : exceptions) {
      stateManager.recordThrowableEvent(exception);
      stateManager.markRunFinished();
    }

    Assertions.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState());

    List<Pair<String, Boolean>> expected = ImmutableList.of(
        Pair.of("java.lang.UnsupportedOperationException", true),
        Pair.of("java.lang.NullPointerException", false),
        Pair.of("java.lang.Exception", true),
        Pair.of("java.lang.IllegalArgumentException", false)
    );

    Iterator<SupervisorStateManager.ExceptionEvent> it = stateManager.getExceptionEvents().iterator();
    expected.forEach(x -> {
      SupervisorStateManager.ExceptionEvent event = it.next();
      Assertions.assertNotNull(event.getMessage());
      Assertions.assertEquals(x.lhs, event.getExceptionClass());
      Assertions.assertEquals(x.rhs, ((SeekableStreamExceptionEvent) event).isStreamException());
    });

    Assertions.assertFalse(it.hasNext());
  }

  @Test
  public void testExceptionEventSerde() throws IOException
  {
    SupervisorStateManager.ExceptionEvent event =
        new SeekableStreamExceptionEvent(new StreamException(new NullPointerException("msg")), true);

    String serialized = defaultMapper.writeValueAsString(event);

    Map<String, String> deserialized = defaultMapper.readValue(serialized, new TypeReference<>() {});
    Assertions.assertNotNull(deserialized.get("timestamp"));
    Assertions.assertEquals("java.lang.NullPointerException", deserialized.get("exceptionClass"));
    Assertions.assertTrue(Boolean.parseBoolean(deserialized.get("streamException")));
    Assertions.assertNotNull(deserialized.get("message"));
  }
}
