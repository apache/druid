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
import org.apache.druid.indexing.seekablestream.SeekableStreamSupervisorConfig;
import org.apache.druid.indexing.seekablestream.exceptions.NonTransientStreamException;
import org.apache.druid.indexing.seekablestream.exceptions.PossiblyTransientStreamException;
import org.apache.druid.indexing.seekablestream.exceptions.TransientStreamException;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorStateManager.StreamErrorTransience;
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
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM);
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM,
        stateManager.getSupervisorState()
    );
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS);
    Assert.assertEquals(
        SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS,
        stateManager.getSupervisorState()
    );
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.CREATING_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.CREATING_TASKS, stateManager.getSupervisorState());
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.RUNNING);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.markRunFinishedAndEvaluateHealth();

    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.CONNECTING_TO_STREAM);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.DISCOVERING_INITIAL_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.CREATING_TASKS);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.RUNNING);
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
    stateManager.markRunFinishedAndEvaluateHealth();
    Assert.assertEquals(SeekableStreamSupervisorStateManager.State.RUNNING, stateManager.getSupervisorState());
  }

  @Test
  public void testTransientStreamFailure()
  {
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.RUNNING);
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

    stateManager.getExceptionEvents().forEach(x -> {
      Assert.assertEquals(StreamErrorTransience.TRANSIENT, x.getStreamErrorTransience());
      Assert.assertEquals(TransientStreamException.class, x.getExceptionClass());
    });
  }

  @Test
  public void testNonTransientStreamFailure()
  {
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.RUNNING);
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

    stateManager.getExceptionEvents().forEach(x -> {
      Assert.assertEquals(StreamErrorTransience.NON_TRANSIENT, x.getStreamErrorTransience());
      Assert.assertEquals(NonTransientStreamException.class, x.getExceptionClass());
    });
  }

  @Test
  public void testPossiblyTransientStreamFailure()
  {
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.RUNNING);
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

    stateManager.getExceptionEvents().forEach(x -> {
      Assert.assertEquals(StreamErrorTransience.POSSIBLY_TRANSIENT, x.getStreamErrorTransience());
      Assert.assertEquals(PossiblyTransientStreamException.class, x.getExceptionClass());
    });
  }

  @Test
  public void testNonTransientUnhealthiness()
  {
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.RUNNING);
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

    stateManager.getExceptionEvents().forEach(x -> {
      Assert.assertEquals(StreamErrorTransience.NON_STREAM_ERROR, x.getStreamErrorTransience());
      Assert.assertEquals(NullPointerException.class, x.getExceptionClass());
    });
  }

  @Test
  public void testTransientUnhealthiness()
  {
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.RUNNING);
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
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.RUNNING);
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

    stateManager.getExceptionEvents().forEach(x -> {
      Assert.assertEquals(StreamErrorTransience.NON_STREAM_ERROR, x.getStreamErrorTransience());
      Assert.assertEquals(Exception.class, x.getExceptionClass());
    });
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
    stateManager.maybeSetState(SeekableStreamSupervisorStateManager.State.RUNNING);
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

    List<Pair<Class, StreamErrorTransience>> expected = ImmutableList.of(
        Pair.of(PossiblyTransientStreamException.class, StreamErrorTransience.POSSIBLY_TRANSIENT),
        Pair.of(NullPointerException.class, StreamErrorTransience.NON_STREAM_ERROR),
        Pair.of(TransientStreamException.class, StreamErrorTransience.TRANSIENT),
        Pair.of(NonTransientStreamException.class, StreamErrorTransience.NON_TRANSIENT),
        Pair.of(TransientStreamException.class, StreamErrorTransience.TRANSIENT)
    );

    Iterator<SeekableStreamSupervisorStateManager.ExceptionEvent> it = stateManager.getExceptionEvents().iterator();
    expected.forEach(x -> {
      SeekableStreamSupervisorStateManager.ExceptionEvent event = it.next();
      Assert.assertNotNull(event.getErrorMessage());
      Assert.assertEquals(x.lhs, event.getExceptionClass());
      Assert.assertEquals(x.rhs, event.getStreamErrorTransience());
    });

    Assert.assertFalse(it.hasNext());
  }

  @Test
  public void testExceptionEventSerDe() throws IOException
  {
    SeekableStreamSupervisorStateManager.ExceptionEvent event =
        new SeekableStreamSupervisorStateManager.ExceptionEvent(
            new NullPointerException("msg"),
            true,
            StreamErrorTransience.TRANSIENT
        );

    String serialized = defaultMapper.writeValueAsString(event);

    Map<String, String> deserialized = defaultMapper.readValue(serialized, new TypeReference<Map<String, String>>(){});
    Assert.assertNotNull(deserialized.get("timestamp"));
    Assert.assertEquals("java.lang.NullPointerException", deserialized.get("exceptionClass"));
    Assert.assertEquals("TRANSIENT", deserialized.get("errorTransience"));
    Assert.assertNotNull(deserialized.get("message"));
  }
}
