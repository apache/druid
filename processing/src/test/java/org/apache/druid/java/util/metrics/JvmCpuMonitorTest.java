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

package org.apache.druid.java.util.metrics;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import java.util.List;

public class JvmCpuMonitorTest
{
  @Test
  public void testDoMonitor()
  {
    final OperatingSystem operatingSystem = Mockito.mock(OperatingSystem.class);
    final OSProcess initialProcess = Mockito.mock(OSProcess.class);
    final OSProcess firstProcess = Mockito.mock(OSProcess.class);
    final OSProcess secondProcess = Mockito.mock(OSProcess.class);
    Mockito.when(operatingSystem.getProcessId()).thenReturn(123);
    Mockito.when(operatingSystem.getProcess(123))
           .thenReturn(initialProcess, firstProcess, secondProcess);
    Mockito.when(firstProcess.getKernelTime()).thenReturn(200L);
    Mockito.when(firstProcess.getUserTime()).thenReturn(300L);
    Mockito.when(secondProcess.getKernelTime()).thenReturn(250L);
    Mockito.when(secondProcess.getUserTime()).thenReturn(450L);
    Mockito.when(firstProcess.getProcessCpuLoadBetweenTicks(initialProcess)).thenReturn(0.25);
    Mockito.when(secondProcess.getProcessCpuLoadBetweenTicks(firstProcess)).thenReturn(0.5);

    final JvmCpuMonitor monitor = new JvmCpuMonitor("test", operatingSystem);
    final StubServiceEmitter emitter = new StubServiceEmitter();

    Assertions.assertTrue(monitor.doMonitor(emitter));
    Assertions.assertTrue(monitor.doMonitor(emitter));

    emitter.verifyValue("jvm/cpu/total", 200L);
    emitter.verifyValue("jvm/cpu/sys", 50L);
    emitter.verifyValue("jvm/cpu/user", 150L);
    Assertions.assertEquals(
        List.of(25.0, 50.0),
        emitter.getMetricValues("jvm/cpu/percent", null)
    );
  }

  @Test
  public void testFailedProcessLookupDoesNotAdvanceBaselines()
  {
    final OperatingSystem operatingSystem = Mockito.mock(OperatingSystem.class);
    final OSProcess initialProcess = Mockito.mock(OSProcess.class);
    final OSProcess firstProcess = Mockito.mock(OSProcess.class);
    final OSProcess secondProcess = Mockito.mock(OSProcess.class);
    Mockito.when(operatingSystem.getProcessId()).thenReturn(123);
    Mockito.when(operatingSystem.getProcess(123))
           .thenReturn(initialProcess, firstProcess)
           .thenReturn(null)
           .thenReturn(secondProcess);
    Mockito.when(firstProcess.getKernelTime()).thenReturn(200L);
    Mockito.when(firstProcess.getUserTime()).thenReturn(300L);
    Mockito.when(secondProcess.getKernelTime()).thenReturn(250L);
    Mockito.when(secondProcess.getUserTime()).thenReturn(450L);
    Mockito.when(firstProcess.getProcessCpuLoadBetweenTicks(initialProcess)).thenReturn(0.25);
    Mockito.when(secondProcess.getProcessCpuLoadBetweenTicks(firstProcess)).thenReturn(0.5);

    final JvmCpuMonitor monitor = new JvmCpuMonitor("test", operatingSystem);
    final StubServiceEmitter emitter = new StubServiceEmitter();

    Assertions.assertTrue(monitor.doMonitor(emitter));
    Assertions.assertTrue(monitor.doMonitor(emitter));
    Assertions.assertTrue(monitor.doMonitor(emitter));

    emitter.verifyValue("jvm/cpu/total", 200L);
    emitter.verifyValue("jvm/cpu/sys", 50L);
    emitter.verifyValue("jvm/cpu/user", 150L);
    Assertions.assertEquals(
        List.of(25.0, 50.0),
        emitter.getMetricValues("jvm/cpu/percent", null)
    );
  }
}
