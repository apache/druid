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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexing.seekablestream.SeekableStreamSupervisorConfig;
import org.apache.druid.indexing.seekablestream.exceptions.NonTransientStreamException;
import org.apache.druid.indexing.seekablestream.exceptions.PossiblyTransientStreamException;
import org.apache.druid.indexing.seekablestream.exceptions.TransientStreamException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Queue;

public class SeekableStreamSupervisorStateManagerTest
{
  private SeekableStreamSupervisorStateManager stateManager;
  private SeekableStreamSupervisorConfig config;
  private ObjectMapper defaultMapper;

  @Before
  public void setupTest()
  {
    config = new SeekableStreamSupervisorConfig();
    config.setMaxStoredExceptionEvents(10);
    stateManager = new SeekableStreamSupervisorStateManager(
        SeekableStreamSupervisorStateManager.State.WAITING_TO_RUN,
        config
    );
    defaultMapper = new DefaultObjectMapper();
  }

  @Test
  public void testHappyPath()
  {
    stateManager.setState(SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM);
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM,
        stateManager.getSupervisorState()
    );
    stateManager.setState(SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS);
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS,
        stateManager.getSupervisorState()
    );
    stateManager.setState(SeekableStreamSupervisorStateManager.State.CREATING_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.CREATING_TASKS, stateManager.getSupervisorState());
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.markRunFinishedAndEvaluateHealth();

    stateManager.setState(SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.setState(SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.setState(SeekableStreamSupervisorStateManager.State.CREATING_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.markRunFinishedAndEvaluateHealth();
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
  }

  @Test
  public void testTransientStreamFailure()
  {
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    stateManager.markRunFinishedAndEvaluateHealth(); // clean run without errors
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
      stateManager.storeThrowableEvent(new PossiblyTransientStreamException(new Exception("DOH!")));
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.LOST_CONTACT_WITH_STREAM,
        stateManager.getSupervisorState()
    );
    Assert.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      SeekableStreamSupervisorStateManager.ExceptionEvent event = stateManager.getExceptionEvents().poll();
      Assert.assertEquals(SeekableStreamSupervisorStateManager.StreamErrorTransience.TRANSIENT, event.getStreamErrorTransience());
      Assert.assertEquals(TransientStreamException.class, event.getExceptionClass());
    }
  }

  @Test
  public void testNonTransientStreamFailure()
  {
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
      stateManager.storeThrowableEvent(new NonTransientStreamException(new Exception("DOH!")));
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.UNABLE_TO_CONNECT_TO_STREAM,
        stateManager.getSupervisorState()
    );
    Assert.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      SeekableStreamSupervisorStateManager.ExceptionEvent event = stateManager.getExceptionEvents().poll();
      Assert.assertEquals(SeekableStreamSupervisorStateManager.StreamErrorTransience.NON_TRANSIENT, event.getStreamErrorTransience());
      Assert.assertEquals(NonTransientStreamException.class, event.getExceptionClass());
    }
  }

  @Test
  public void testPossiblyTransientStreamFailure()
  {
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
      stateManager.storeThrowableEvent(new PossiblyTransientStreamException(new Exception("DOH!")));
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.UNABLE_TO_CONNECT_TO_STREAM,
        stateManager.getSupervisorState()
    );
    Assert.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      SeekableStreamSupervisorStateManager.ExceptionEvent event = stateManager.getExceptionEvents().poll();
      Assert.assertEquals(SeekableStreamSupervisorStateManager.StreamErrorTransience.POSSIBLY_TRANSIENT, event.getStreamErrorTransience());
      Assert.assertEquals(PossiblyTransientStreamException.class, event.getExceptionClass());
    }
  }

  @Test
  public void testNonTransientUnhealthiness()
  {
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
      stateManager.storeThrowableEvent(new NullPointerException("oof"));
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.UNHEALTHY_SUPERVISOR,
        stateManager.getSupervisorState()
    );
    Assert.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      SeekableStreamSupervisorStateManager.ExceptionEvent event = stateManager.getExceptionEvents().poll();
      Assert.assertEquals(SeekableStreamSupervisorStateManager.StreamErrorTransience.NON_STREAM_ERROR, event.getStreamErrorTransience());
      Assert.assertEquals(NullPointerException.class, event.getExceptionClass());
    }
  }

  @Test
  public void testTransientUnhealthiness()
  {
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    for (int i = 0; i < config.getUnhealthinessThreshold() - 1; i++) {
      stateManager.storeThrowableEvent(new NullPointerException("oof"));
      stateManager.markRunFinishedAndEvaluateHealth();
      Assert.assertEquals(
          SeekableStreamSupervisorStateManager.State.RUNNING,
          stateManager.getSupervisorState()
      );
    }
    stateManager.markRunFinishedAndEvaluateHealth(); // clean run
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.RUNNING,
        stateManager.getSupervisorState()
    );
    Assert.assertEquals(config.getUnhealthinessThreshold() - 1, stateManager.getExceptionEvents().size());
  }

  @Test
  public void testNonTransientTaskUnhealthiness()
  {
    for (int i = 0; i < config.getTaskUnhealthinessThreshold(); i++) {
      Assert.assertNotEquals(
          SeekableStreamSupervisorStateManager.State.UNHEALTHY_TASKS,
          stateManager.getSupervisorState()
      );
      stateManager.storeCompletedTaskState(TaskState.FAILED);
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.UNHEALTHY_TASKS,
        stateManager.getSupervisorState()
    );
    Assert.assertEquals(0, stateManager.getExceptionEvents().size());
  }

  @Test
  public void testTransientTaskUnhealthiness()
  {
    // Only half are failing
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    for (int i = 0; i < config.getTaskUnhealthinessThreshold() + 3; i++) {
      Assert.assertNotEquals(
          stateManager.getSupervisorState(),
          SeekableStreamSupervisorStateManager.State.UNHEALTHY_TASKS
      );
      stateManager.storeCompletedTaskState(TaskState.FAILED);
      stateManager.storeCompletedTaskState(TaskState.SUCCESS);
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.RUNNING,
        stateManager.getSupervisorState()
    );
    Assert.assertEquals(0, stateManager.getExceptionEvents().size());
  }

  @Test
  public void testSupervisorRecoveryWithHealthinessThreshold()
  {
    // Put into an unhealthy state
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      Assert.assertNotEquals(
          SeekableStreamSupervisorStateManager.State.UNHEALTHY_SUPERVISOR,
          stateManager.getSupervisorState()
      );
      stateManager.storeThrowableEvent(new Exception("Except the inevitable"));
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.UNHEALTHY_SUPERVISOR,
        stateManager.getSupervisorState()
    );
    // Recover after config.healthinessThreshold successful task completions
    for (int i = 0; i < config.getHealthinessThreshold(); i++) {
      Assert.assertEquals(
          SeekableStreamSupervisorStateManager.State.UNHEALTHY_SUPERVISOR,
          stateManager.getSupervisorState()
      );
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.RUNNING,
        stateManager.getSupervisorState()
    );
    Assert.assertEquals(config.getUnhealthinessThreshold(), stateManager.getExceptionEvents().size());
    for (int i = 0; i < config.getUnhealthinessThreshold(); i++) {
      SeekableStreamSupervisorStateManager.ExceptionEvent event = stateManager.getExceptionEvents().poll();
      Assert.assertEquals(
          SeekableStreamSupervisorStateManager.StreamErrorTransience.NON_STREAM_ERROR,
          event.getStreamErrorTransience()
      );
      Assert.assertEquals(Exception.class, event.getExceptionClass());
    }
  }

  @Test
  public void testTaskRecoveryWithHealthinessThreshold()
  {
    // Put into an unhealthy state
    for (int i = 0; i < config.getTaskUnhealthinessThreshold(); i++) {
      Assert.assertNotEquals(
          SeekableStreamSupervisorStateManager.State.UNHEALTHY_TASKS,
          stateManager.getSupervisorState()
      );
      stateManager.storeCompletedTaskState(TaskState.FAILED);
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.UNHEALTHY_TASKS,
        stateManager.getSupervisorState()
    );
    // Recover after config.healthinessThreshold successful task completions
    for (int i = 0; i < config.getTaskHealthinessThreshold(); i++) {
      Assert.assertEquals(
          SeekableStreamSupervisorStateManager.State.UNHEALTHY_TASKS,
          stateManager.getSupervisorState()
      );
      stateManager.storeCompletedTaskState(TaskState.SUCCESS);
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.RUNNING,
        stateManager.getSupervisorState()
    );
  }

  @Test
  public void testTwoUnhealthyStates()
  {
    stateManager.setState(SeekableStreamSupervisorStateManager.State.RUNNING);
    for (int i = 0; i < Math.max(
        config.getTaskUnhealthinessThreshold(),
        config.getUnhealthinessThreshold()
    ); i++) {
      stateManager.storeThrowableEvent(new NullPointerException("somebody goofed"));
      stateManager.storeCompletedTaskState(TaskState.FAILED);
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    // UNHEALTHY_SUPERVISOR should take priority over UNHEALTHY_TASKS
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.UNHEALTHY_SUPERVISOR,
        stateManager.getSupervisorState()
    );
  }

  @Test
  public void testGetThrowableEvents()
  {
    List<Exception> exceptions = ImmutableList.of(
        new PossiblyTransientStreamException(new Exception("oof")),
        new NullPointerException("oof"),
        new TransientStreamException(new Exception("oof")),
        new NonTransientStreamException(new Exception("oof"))
    );
    for (Exception exception : exceptions) {
      stateManager.storeThrowableEvent(exception);
      stateManager.markRunFinishedAndEvaluateHealth();
    }
    stateManager.markRunFinishedAndEvaluateHealth();
    stateManager.storeThrowableEvent(new PossiblyTransientStreamException(new Exception("oof")));
    Queue<SeekableStreamSupervisorStateManager.ExceptionEvent> events = stateManager.getExceptionEvents();

    Assert.assertNotNull(events.peek().getErrorMessage());
    Assert.assertEquals(PossiblyTransientStreamException.class, events.poll().getExceptionClass());
    Assert.assertNotNull(events.peek().getErrorMessage());
    Assert.assertEquals(NullPointerException.class, events.poll().getExceptionClass());
    Assert.assertNotNull(events.peek().getErrorMessage());
    Assert.assertEquals(TransientStreamException.class, events.poll().getExceptionClass());
    Assert.assertNotNull(events.peek().getErrorMessage());
    Assert.assertEquals(NonTransientStreamException.class, events.poll().getExceptionClass());
    Assert.assertNotNull(events.peek().getErrorMessage());
    Assert.assertEquals(TransientStreamException.class, events.poll().getExceptionClass());
  }

  @Test
  public void testExceptionEventSerDe() throws IOException
  {
    SeekableStreamSupervisorStateManager.ExceptionEvent event =
        new SeekableStreamSupervisorStateManager.ExceptionEvent(
            new NullPointerException("msg"),
            true,
            SeekableStreamSupervisorStateManager.StreamErrorTransience.TRANSIENT
        );
    String serialized = defaultMapper.writeValueAsString(event);
    SeekableStreamSupervisorStateManager.ExceptionEvent deserialized =
        defaultMapper.readValue(serialized, SeekableStreamSupervisorStateManager.ExceptionEvent.class);
    Assert.assertEquals(event.getErrorMessage(), deserialized.getErrorMessage());
    Assert.assertEquals(event.getExceptionClass(), deserialized.getExceptionClass());
    Assert.assertEquals(event.getTimestamp(), deserialized.getTimestamp());
    Assert.assertEquals(event.getStreamErrorTransience(), deserialized.getStreamErrorTransience());
  }
}
