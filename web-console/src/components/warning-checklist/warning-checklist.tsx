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

import { Checkbox } from '@blueprintjs/core';
import React, { useState } from 'react';

export interface WarningChecklistProps {
  checks: string[];
  onChange: (allChecked: boolean) => void;
}

export const WarningChecklist = React.memo(function WarningChecklist(props: WarningChecklistProps) {
  const { checks, onChange } = props;
  const [checked, setChecked] = useState<Record<string, boolean>>({});

  function doCheck(check: string) {
    const newChecked = Object.assign({}, checked);
    newChecked[check] = !newChecked[check];
    setChecked(newChecked);

    onChange(checks.every(check => newChecked[check]));
  }

  return (
    <div className="warning-checklist">
      {checks.map((check, i) => {
        return <Checkbox key={i} label={check} onChange={() => doCheck(check)} />;
      })}
    </div>
  );
});
