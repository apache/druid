
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

import * as React from 'react';

import './table-cell.scss';

export interface NullTableCellProps extends React.Props<any> {
  value?: any;
  timestamp?: boolean;
  unparseable?: boolean;
}

export class TableCell extends React.Component<NullTableCellProps, {}> {
  render() {
    const { value, timestamp, unparseable } = this.props;
    if (unparseable) {
      return <span className="table-cell unparseable">error</span>;
    } else if (value !== '' && value != null) {
      if (timestamp) {
        return <span className="table-cell timestamp" title={value}>{new Date(value).toISOString()}</span>;
      } else if (Array.isArray(value)) {
        return `[${value.join(', ')}]`;
      } else {
        return value;
      }
    } else {
      return <span className="table-cell null">null</span>;
    }
  }
}
