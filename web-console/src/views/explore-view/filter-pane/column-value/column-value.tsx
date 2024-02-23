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

import './column-value.scss';

export interface ColumnValueProps {
  value: any;
}

export const ColumnValue = function ColumnValue(props: ColumnValueProps) {
  const { value } = props;

  if (value === '') {
    return <span className="column-value empty">empty</span>;
  } else if (value === null) {
    return <span className="column-value null">null</span>;
  }

  return <span className="column-value">{String(value)}</span>;
};
