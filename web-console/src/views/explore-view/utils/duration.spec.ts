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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { formatDuration } from './duration';

describe('formatDuration', () => {
  it('works with 0', () => {
    expect(formatDuration('PT0S')).toEqual('0 seconds');
  });

  it('works with single span', () => {
    expect(formatDuration('P1D')).toEqual('1 day');
    expect(formatDuration('PT1M')).toEqual('1 minute');
  });

  it('works with single span (compact)', () => {
    expect(formatDuration('PT1M', true)).toEqual('minute');
  });

  it('works with multiple spans', () => {
    expect(formatDuration('PT2H30M15S')).toEqual('2 hours, 30 minutes, 15 seconds');
    expect(formatDuration('PT2H30M15S', true)).toEqual('2 hours, 30 minutes, 15 seconds');
  });
});
