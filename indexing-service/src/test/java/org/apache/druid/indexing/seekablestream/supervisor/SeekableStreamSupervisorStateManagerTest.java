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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SeekableStreamSupervisorStateManagerTest
{
  private SeekableStreamSupervisorStateManager stateManager;
  private SupervisorStateManagerConfig config;
  private ObjectMapper defaultMapper;

  @Before
  public void setupTest()
  {
    config = new SupervisorStateManagerConfig(10);
    stateManager = new SeekableStreamSupervisorStateManager(config, false);
    defaultMapper = new DefaultObjectMapper();
  }

  @Test
  public void testHappyPath()
  {
    Assert.assertEquals(BasicState.PENDING, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.PENDING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.SeekableStreamState.CONNECTING_TO_STREAM);
    Assert.assertEquals(SeekableStreamState.CONNECTING_TO_STREAM, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.DISCOVERING_INITIAL_TASKS);
    Assert.assertEquals(SeekableStreamState.DISCOVERING_INITIAL_TASKS, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.CREATING_TASKS);
    Assert.assertEquals(SeekableStreamState.CREATING_TASKS, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.markRunFinished();
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());


    stateManager.maybeSetState(BasicState.PENDING);
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.CONNECTING_TO_STREAM);
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.DISCOVERING_INITIAL_TASKS);
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.maybeSetState(SeekableStreamState.CREATING_TASKS);
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());

    stateManager.markRunFinished();
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());
  }

  @Test
  public void testStreamFailureLostContact()
  {
    stateManager.markRunFinished(); // clean run without errors

    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());

    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      stateManager.recordThrowableEvent(new StreamException(new IllegalStateException("DOH!")));
      stateManager.markRunFinished();
    }
    Assert.assertEquals(SeekableStreamState.LOST_CONTACT_WITH_STREAM, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());

    stateManager.getExceptionEvents().forEach(x -> {
      Assert.assertTrue(((SeekableStreamExceptionEvent) x).isStreamException());
      Assert.assertEquals(IllegalStateException.class.getName(), x.getExceptionClass());
    });
  }

  @Test
  public void testStreamFailureUnableToConnect()
  {
    stateManager.maybeSetState(SeekableStreamState.CONNECTING_TO_STREAM);
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assert.assertEquals(SeekableStreamState.CONNECTING_TO_STREAM, stateManager.getSupervisorState());
      stateManager.recordThrowableEvent(new StreamException(new IllegalStateException("DOH!")));
      stateManager.markRunFinished();
    }
    Assert.assertEquals(SeekableStreamState.UNABLE_TO_CONNECT_TO_STREAM, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());

    stateManager.getExceptionEvents().forEach(x -> {
      Assert.assertTrue(((SeekableStreamExceptionEvent) x).isStreamException());
      Assert.assertEquals(IllegalStateException.class.getName(), x.getExceptionClass());
    });
  }

  @Test
  public void testNonStreamUnhealthiness()
  {
    stateManager.maybeSetState(SeekableStreamState.DISCOVERING_INITIAL_TASKS);
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assert.assertEquals(SeekableStreamState.DISCOVERING_INITIAL_TASKS, stateManager.getSupervisorState());
      stateManager.recordThrowableEvent(new NullPointerException("oof"));
      stateManager.markRunFinished();
    }
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());

    stateManager.getExceptionEvents().forEach(x -> {
      Assert.assertFalse(((SeekableStreamExceptionEvent) x).isStreamException());
      Assert.assertEquals(NullPointerException.class.getName(), x.getExceptionClass());
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
        Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      }

      stateManager.markRunFinished(); // clean run
      Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());
      Assert.assertEquals(j * (config.getUnhealthinessThreshold() - 1), stateManager.getExceptionEvents().size());
    }
  }

  @Test
  public void testNonTransientTaskUnhealthiness()
  {
    stateManager.markRunFinished();
    for (int i = 0; i < config.getTaskUnhealthinessThreshold(); i++) {
      Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      stateManager.recordCompletedTaskState(TaskState.FAILED);
      stateManager.markRunFinished();
    }
    Assert.assertEquals(BasicState.UNHEALTHY_TASKS, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.UNHEALTHY_TASKS, stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(0, stateManager.getExceptionEvents().size());
  }

  @Test
  public void testTransientTaskUnhealthiness()
  {
    // Only half are failing
    stateManager.markRunFinished();
    for (int i = 0; i < config.getTaskUnhealthinessThreshold() + 3; i++) {
      Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      stateManager.recordCompletedTaskState(TaskState.FAILED);
      stateManager.recordCompletedTaskState(TaskState.SUCCESS);
      stateManager.markRunFinished();
    }
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState().getBasicState());
    Assert.assertEquals(0, stateManager.getExceptionEvents().size());
  }

  @Test
  public void testSupervisorRecoveryWithHealthinessThreshold()
  {
    // Put into an unhealthy state
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assert.assertEquals(BasicState.PENDING, stateManager.getSupervisorState());
      stateManager.recordThrowableEvent(new Exception("Except the inevitable"));
      stateManager.markRunFinished();
    }
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState());

    // Recover after config.healthinessThreshold successful task completions
    for (int i = 0; i < config.getHealthinessThreshold(); i++) {
      Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState());
      stateManager.markRunFinished();
    }
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
    Assert.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());

    stateManager.getExceptionEvents().forEach(x -> {
      Assert.assertFalse(((SeekableStreamExceptionEvent) x).isStreamException());
      Assert.assertEquals(Exception.class.getName(), x.getExceptionClass());
    });
  }

  @Test
  public void testTaskRecoveryWithHealthinessThreshold()
  {
    stateManager.markRunFinished();

    // Put into an unhealthy state
    for (int i = 0; i < config.getTaskUnhealthinessThreshold(); i++) {
      Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
      stateManager.recordCompletedTaskState(TaskState.FAILED);
      stateManager.markRunFinished();
    }
    Assert.assertEquals(BasicState.UNHEALTHY_TASKS, stateManager.getSupervisorState());

    // Recover after config.healthinessThreshold successful task completions
    for (int i = 0; i < config.getTaskHealthinessThreshold(); i++) {
      Assert.assertEquals(BasicState.UNHEALTHY_TASKS, stateManager.getSupervisorState());
      stateManager.recordCompletedTaskState(TaskState.SUCCESS);
      stateManager.markRunFinished();
    }
    Assert.assertEquals(BasicState.RUNNING, stateManager.getSupervisorState());
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
    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState());
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

    Assert.assertEquals(BasicState.UNHEALTHY_SUPERVISOR, stateManager.getSupervisorState());

    List<Pair<String, Boolean>> expected = ImmutableList.of(
        Pair.of("java.lang.UnsupportedOperationException", true),
        Pair.of("java.lang.NullPointerException", false),
        Pair.of("java.lang.Exception", true),
        Pair.of("java.lang.IllegalArgumentException", false)
    );

    Iterator<SupervisorStateManager.ExceptionEvent> it = stateManager.getExceptionEvents().iterator();
    expected.forEach(x -> {
      SupervisorStateManager.ExceptionEvent event = it.next();
      Assert.assertNotNull(event.getMessage());
      Assert.assertEquals(x.lhs, event.getExceptionClass());
      Assert.assertEquals(x.rhs, ((SeekableStreamExceptionEvent) event).isStreamException());
    });

    Assert.assertFalse(it.hasNext());
  }

  @Test
  public void testExceptionEventSerde() throws IOException
  {
    SupervisorStateManager.ExceptionEvent event =
        new SupervisorStateManager.ExceptionEvent(new NullPointerException("msg"), true);

    String serialized = defaultMapper.writeValueAsString(event);

    Map<String, String> deserialized = defaultMapper.readValue(serialized, new TypeReference<Map<String, String>>()
    {
    });
    Assert.assertNotNull(deserialized.get("timestamp"));
    Assert.assertEquals("java.lang.NullPointerException", deserialized.get("exceptionClass"));
    Assert.assertFalse(Boolean.getBoolean(deserialized.get("streamException")));
    Assert.assertNotNull(deserialized.get("message"));
  }
}
