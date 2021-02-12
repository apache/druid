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

import { IconNames } from '@blueprintjs/icons';
import * as JSONBig from 'json-bigint-native';
import React, { useState } from 'react';

import { ShowValueDialog } from '../../dialogs/show-value-dialog/show-value-dialog';
import { ActionIcon } from '../action-icon/action-icon';

import './table-cell.scss';

const MAX_CHARS_TO_SHOW = 50;
const ABSOLUTE_MAX_CHARS_TO_SHOW = 5000;

interface ShortParts {
  prefix: string;
  omitted: string;
  suffix: string;
}

function shortenString(str: string): ShortParts {
  // Print something like:
  // BAAAArAAEiQKpDAEAACwZCBAGSBgiSEAAAAQpAIDwAg...23 omitted...gwiRoQBJIC
  const omit = str.length - (MAX_CHARS_TO_SHOW - 17);
  const prefix = str.substr(0, str.length - (omit + 10));
  const suffix = str.substr(str.length - 10);
  return {
    prefix,
    omitted: `...${omit} omitted...`,
    suffix,
  };
}

export interface TableCellProps {
  value: any;
  unlimited?: boolean;
}

export const TableCell = React.memo(function TableCell(props: TableCellProps) {
  const { value, unlimited } = props;
  const [showValue, setShowValue] = useState<string | undefined>();

  function renderShowValueDialog(): JSX.Element | undefined {
    if (!showValue) return;

    return <ShowValueDialog onClose={() => setShowValue(undefined)} str={showValue} />;
  }

  function renderTruncated(str: string): JSX.Element {
    if (str.length <= MAX_CHARS_TO_SHOW) {
      return <span className="table-cell plain">{str}</span>;
    }

    if (unlimited) {
      return (
        <span className="table-cell plain">
          {str.length < ABSOLUTE_MAX_CHARS_TO_SHOW
            ? str
            : `${str.substr(0, ABSOLUTE_MAX_CHARS_TO_SHOW)}...`}
        </span>
      );
    }

    const { prefix, omitted, suffix } = shortenString(str);
    return (
      <span className="table-cell truncated">
        {prefix}
        <span className="omitted">{omitted}</span>
        {suffix}
        <ActionIcon icon={IconNames.MORE} onClick={() => setShowValue(str)} />
        {renderShowValueDialog()}
      </span>
    );
  }

  if (value !== '' && value != null) {
    if (value instanceof Date) {
      return (
        <span className="table-cell timestamp" title={String(value.valueOf())}>
          {value.toISOString()}
        </span>
      );
    } else if (Array.isArray(value)) {
      return renderTruncated(`[${value.join(', ')}]`);
    } else if (typeof value === 'object') {
      return renderTruncated(JSONBig.stringify(value));
    } else {
      return renderTruncated(String(value));
    }
  } else {
    return <span className="table-cell null">null</span>;
  }
});
