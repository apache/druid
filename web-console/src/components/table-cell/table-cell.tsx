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
import React from 'react';

import { ShowValueDialog } from '../../dialogs/show-value-dialog/show-value-dialog';
import { ActionIcon } from '../action-icon/action-icon';

import './table-cell.scss';

export interface TableCellProps {
  value?: any;
  timestamp?: boolean;
  unparseable?: boolean;
}

export interface TableCellState {
  showValue?: string;
}

interface ShortParts {
  prefix: string;
  omitted: string;
  suffix: string;
}

export class TableCell extends React.PureComponent<TableCellProps, TableCellState> {
  static MAX_CHARS_TO_SHOW = 50;

  static shortenString(str: string): ShortParts {
    // Print something like:
    // BAAAArAAEiQKpDAEAACwZCBAGSBgiSEAAAAQpAIDwAg...23 omitted...gwiRoQBJIC
    const omit = str.length - (TableCell.MAX_CHARS_TO_SHOW - 17);
    const prefix = str.substr(0, str.length - (omit + 10));
    const suffix = str.substr(str.length - 10);
    return {
      prefix,
      omitted: `...${omit} omitted...`,
      suffix,
    };
  }

  constructor(props: TableCellProps) {
    super(props);
    this.state = {};
  }

  private renderShowValueDialog(): JSX.Element | undefined {
    const { showValue } = this.state;
    if (!showValue) return;

    return (
      <ShowValueDialog onClose={() => this.setState({ showValue: undefined })} str={showValue} />
    );
  }

  private renderTruncated(str: string): React.ReactNode {
    if (str.length <= TableCell.MAX_CHARS_TO_SHOW) return str;

    const { prefix, omitted, suffix } = TableCell.shortenString(str);
    return (
      <span className="table-cell truncated">
        {prefix}
        <span className="omitted">{omitted}</span>
        {suffix}
        <ActionIcon icon={IconNames.MORE} onClick={() => this.setState({ showValue: str })} />
        {this.renderShowValueDialog()}
      </span>
    );
  }

  render(): React.ReactNode {
    const { value, timestamp, unparseable } = this.props;
    if (unparseable) {
      return <span className="table-cell unparseable">error</span>;
    } else if (value !== '' && value != null) {
      if (timestamp) {
        return (
          <span className="table-cell timestamp" title={value}>
            {new Date(value).toISOString()}
          </span>
        );
      } else if (Array.isArray(value)) {
        return this.renderTruncated(`[${value.join(', ')}]`);
      } else {
        return this.renderTruncated(String(value));
      }
    } else {
      if (timestamp) {
        return <span className="table-cell unparseable">unparseable timestamp</span>;
      } else {
        return <span className="table-cell null">null</span>;
      }
    }
  }
}
