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

package org.apache.druid.msq.exec;

import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.indexing.error.MSQException;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

public class RunWorkOrderTest
{
  private static final String CANCELLATION_ID = "my-cancellation-id";

  @Test
  public void test_stopUnchecked() throws InterruptedException
  {
    final FrameProcessorExecutor exec = Mockito.mock(FrameProcessorExecutor.class);
    final WorkerContext workerContext = Mockito.mock(WorkerContext.class);
    final FrameContext frameContext = Mockito.mock(FrameContext.class);
    final WorkerStorageParameters storageParameters = Mockito.mock(WorkerStorageParameters.class);
    final RunWorkOrderListener listener = Mockito.mock(RunWorkOrderListener.class);

    Mockito.when(frameContext.storageParameters()).thenReturn(storageParameters);

    final RunWorkOrder runWorkOrder =
        new RunWorkOrder(null, null, null, exec, CANCELLATION_ID, workerContext, frameContext, listener);

    runWorkOrder.stopUnchecked(null);

    // Calling a second time doesn't do anything special.
    runWorkOrder.stopUnchecked(null);

    Mockito.verify(exec).cancel(CANCELLATION_ID);
    Mockito.verify(frameContext).close();
    Mockito.verify(listener).onFailure(ArgumentMatchers.any(MSQException.class));
  }

  @Test
  public void test_stopUnchecked_error() throws InterruptedException
  {
    final FrameProcessorExecutor exec = Mockito.mock(FrameProcessorExecutor.class);
    final WorkerContext workerContext = Mockito.mock(WorkerContext.class);
    final FrameContext frameContext = Mockito.mock(FrameContext.class);
    final WorkerStorageParameters storageParameters = Mockito.mock(WorkerStorageParameters.class);
    final RunWorkOrderListener listener = Mockito.mock(RunWorkOrderListener.class);

    Mockito.when(frameContext.storageParameters()).thenReturn(storageParameters);

    final RunWorkOrder runWorkOrder =
        new RunWorkOrder(null, null, null, exec, CANCELLATION_ID, workerContext, frameContext, listener);

    final ISE exception = new ISE("oops");

    Assert.assertThrows(
        IllegalStateException.class,
        () -> runWorkOrder.stopUnchecked(exception)
    );

    // Calling a second time doesn't do anything special. We already tried our best.
    runWorkOrder.stopUnchecked(null);

    Mockito.verify(exec).cancel(CANCELLATION_ID);
    Mockito.verify(frameContext).close();
    Mockito.verify(listener).onFailure(ArgumentMatchers.eq(exception));
  }

  @Test
  public void test_stopUnchecked_errorDuringExecCancel() throws InterruptedException
  {
    final FrameProcessorExecutor exec = Mockito.mock(FrameProcessorExecutor.class);
    final WorkerContext workerContext = Mockito.mock(WorkerContext.class);
    final FrameContext frameContext = Mockito.mock(FrameContext.class);
    final WorkerStorageParameters storageParameters = Mockito.mock(WorkerStorageParameters.class);
    final RunWorkOrderListener listener = Mockito.mock(RunWorkOrderListener.class);

    final ISE exception = new ISE("oops");
    Mockito.when(frameContext.storageParameters()).thenReturn(storageParameters);
    Mockito.doThrow(exception).when(exec).cancel(CANCELLATION_ID);

    final RunWorkOrder runWorkOrder =
        new RunWorkOrder(null, null, null, exec, CANCELLATION_ID, workerContext, frameContext, listener);

    Assert.assertThrows(
        IllegalStateException.class,
        () -> runWorkOrder.stopUnchecked(null)
    );

    Mockito.verify(exec).cancel(CANCELLATION_ID);
    Mockito.verify(frameContext).close();
    Mockito.verify(listener).onFailure(ArgumentMatchers.eq(exception));
  }

  @Test
  public void test_stopUnchecked_errorDuringFrameContextClose() throws InterruptedException
  {
    final FrameProcessorExecutor exec = Mockito.mock(FrameProcessorExecutor.class);
    final WorkerContext workerContext = Mockito.mock(WorkerContext.class);
    final FrameContext frameContext = Mockito.mock(FrameContext.class);
    final WorkerStorageParameters storageParameters = Mockito.mock(WorkerStorageParameters.class);
    final RunWorkOrderListener listener = Mockito.mock(RunWorkOrderListener.class);

    final ISE exception = new ISE("oops");
    Mockito.when(frameContext.storageParameters()).thenReturn(storageParameters);
    Mockito.doThrow(exception).when(frameContext).close();

    final RunWorkOrder runWorkOrder =
        new RunWorkOrder(null, null, null, exec, CANCELLATION_ID, workerContext, frameContext, listener);

    Assert.assertThrows(
        IllegalStateException.class,
        () -> runWorkOrder.stopUnchecked(null)
    );

    Mockito.verify(exec).cancel(CANCELLATION_ID);
    Mockito.verify(frameContext).close();
    Mockito.verify(listener).onFailure(ArgumentMatchers.eq(exception));
  }
}
