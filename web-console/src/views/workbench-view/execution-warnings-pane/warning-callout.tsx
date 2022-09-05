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

import { Button, Callout, Collapse } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { ExecutionError } from '../../../druid-models';

import './warning-callout.scss';

interface WarningCalloutProps {
  warning: ExecutionError;
}

export const WarningCallout = React.memo(function WarningCallout(props: WarningCalloutProps) {
  const { warning } = props;

  const [showStack, setShowStack] = useState(false);

  return (
    <Callout
      className="warning-callout"
      icon={IconNames.WARNING_SIGN}
      title={warning.error.errorCode}
    >
      <p>{warning.error.errorMessage}</p>
      <Button
        text={showStack ? 'Hide stack' : 'Show stack'}
        rightIcon={showStack ? IconNames.CARET_UP : IconNames.CARET_DOWN}
        onClick={() => setShowStack(!showStack)}
      />
      <Collapse isOpen={showStack}>
        <pre>{warning.exceptionStackTrace}</pre>
      </Collapse>
    </Callout>
  );
});
