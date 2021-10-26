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

import { Button } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { useInterval } from '../../../hooks';

import './query-timer.scss';

// This is roughly the time in ms that it takes the component to mount and unmount, without this the timer appears to over count a little bit
const FUDGE_OFFSET = 150;

export const QueryTimer = React.memo(function QueryTimer() {
  const [startTime] = useState(Date.now() + FUDGE_OFFSET);
  const [currentTime, setCurrentTime] = useState(Date.now());

  useInterval(() => {
    setCurrentTime(Date.now());
  }, 25);

  const elapsed = currentTime - startTime;
  if (elapsed <= 0) return null;
  return (
    <div className="query-timer">
      {`${(elapsed / 1000).toFixed(2)}s`}
      <Button icon={IconNames.STOPWATCH} minimal />
    </div>
  );
});
