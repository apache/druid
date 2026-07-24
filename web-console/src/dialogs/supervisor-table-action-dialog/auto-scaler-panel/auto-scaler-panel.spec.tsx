/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific
 * language governing permissions and limitations under the License.
 */

import { getAutoScalerValidationError } from './auto-scaler-panel';

describe('getAutoScalerValidationError', () => {
  const validValues = {
    taskCountMin: 1,
    taskCountMax: 10,
    maxProcessingRatePerTask: 100,
    optimalTaskIdleRatio: 0.2,
    criticalLag: 1000,
    currentTaskCount: undefined,
  };

  it('validates the simulator inputs before making a request', () => {
    expect(getAutoScalerValidationError(validValues)).toBeUndefined();
    expect(getAutoScalerValidationError({ ...validValues, taskCountMin: 11 })).toBeDefined();
    expect(
      getAutoScalerValidationError({ ...validValues, maxProcessingRatePerTask: 99 }),
    ).toBeDefined();
    expect(getAutoScalerValidationError({ ...validValues, optimalTaskIdleRatio: 0 })).toBeDefined();
    expect(getAutoScalerValidationError({ ...validValues, criticalLag: 999 })).toBeDefined();
    expect(getAutoScalerValidationError({ ...validValues, currentTaskCount: 11 })).toBeDefined();
  });
});
