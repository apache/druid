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

import React from 'react';

import { Execution } from '../../../druid-models';

import { WarningCallout } from './warning-callout';

import './execution-warnings-pane.scss';

interface ExecutionWarningsPaneProps {
  execution: Execution;
}

export const ExecutionWarningsPane = React.memo(function ExecutionWarningsPane(
  props: ExecutionWarningsPaneProps,
) {
  const { execution } = props;
  const warnings = execution.warnings;
  if (!warnings) return null;

  return (
    <div className="execution-warnings-pane">
      {warnings.map((warning, i) => {
        return <WarningCallout key={i} warning={warning} />;
      })}
    </div>
  );
});
