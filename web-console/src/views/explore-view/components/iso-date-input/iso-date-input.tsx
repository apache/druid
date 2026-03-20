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

import { InputGroup, Intent } from '@blueprintjs/core';
import { useState } from 'react';

import { parseIsoDate } from '../../../../utils';

function normalizeDateString(dateString: string): string {
  return dateString.replace(/[^\-0-9T:./Z ]/g, '');
}

function tryParseIsoDate(dateString: string): Date | undefined {
  try {
    return parseIsoDate(dateString);
  } catch {
    return undefined;
  }
}

function formatDate(date: Date): string {
  return date.toISOString().replace(/Z$/, '').replace('.000', '').replace(/T/g, ' ');
}

export interface UtcDateInputProps {
  date: Date;
  onChange(newDate: Date): void;
  onIssue(issue: string): void;
}

export function IsoDateInput(props: UtcDateInputProps) {
  const { date, onChange, onIssue } = props;
  const [invalidDateString, setInvalidDateString] = useState<string | undefined>();
  const [customDateString, setCustomDateString] = useState<string | undefined>();
  const [focused, setFocused] = useState<boolean>(false);

  return (
    <InputGroup
      className="iso-date-input"
      placeholder="yyyy-MM-dd HH:mm:ss"
      intent={!focused && invalidDateString ? Intent.DANGER : undefined}
      value={
        invalidDateString ??
        (customDateString && tryParseIsoDate(customDateString)?.valueOf() === date.valueOf()
          ? customDateString
          : undefined) ??
        formatDate(date)
      }
      onChange={e => {
        const normalizedDateString = normalizeDateString(e.target.value);
        try {
          const parsedDate = parseIsoDate(normalizedDateString);
          onChange(parsedDate);
          setInvalidDateString(undefined);
          setCustomDateString(normalizedDateString);
        } catch (e) {
          onIssue(e.message);
          setInvalidDateString(normalizedDateString);
          setCustomDateString(undefined);
        }
      }}
      onFocus={() => setFocused(true)}
      onBlur={() => setFocused(false)}
    />
  );
}
