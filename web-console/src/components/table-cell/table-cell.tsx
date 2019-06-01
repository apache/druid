
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

import { Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import * as copy from 'copy-to-clipboard';
import * as React from 'react';

import { AppToaster } from '../../singletons/toaster';
import { ActionIcon } from '../action-icon/action-icon';

import './table-cell.scss';

export interface NullTableCellProps extends React.Props<any> {
  value?: any;
  timestamp?: boolean;
  unparseable?: boolean;
}

export class TableCell extends React.Component<NullTableCellProps, {}> {
  static MAX_CHARS_TO_SHOW = 50;

  static possiblyTruncate(str: string): React.ReactNode {
    if (str.length < TableCell.MAX_CHARS_TO_SHOW) return str;
    return <span className="table-cell truncated">
      {TableCell.shortenString(str)}
      <ActionIcon
        icon={IconNames.CLIPBOARD}
        onClick={() => {
          copy(str, { format: 'text/plain' });
          AppToaster.show({
            message: 'Value copied to clipboard',
            intent: Intent.SUCCESS
          });
        }}
      />
    </span>;
  }

  static shortenString(str: string): string {
    // Print something like:
    // BAAAArAAEiQKpDAEAACwZCBAGSBgiSEAAAAQpAIDwAg...23 omitted...gwiRoQBJIC
    const omit = str.length - (TableCell.MAX_CHARS_TO_SHOW + 17);
    const prefix = str.substr(0, str.length - (omit + 10));
    const suffix = str.substr(str.length - 10);
    return `${prefix} ...${omit} omitted... ${suffix}`;
  }

  render() {
    const { value, timestamp, unparseable } = this.props;
    if (unparseable) {
      return <span className="table-cell unparseable">error</span>;
    } else if (value !== '' && value != null) {
      if (timestamp) {
        return <span className="table-cell timestamp" title={value}>{new Date(value).toISOString()}</span>;
      } else if (Array.isArray(value)) {
        return TableCell.possiblyTruncate(`[${value.join(', ')}]`);
      } else {
        return TableCell.possiblyTruncate(String(value));
      }
    } else {
      return <span className="table-cell null">null</span>;
    }
  }
}
