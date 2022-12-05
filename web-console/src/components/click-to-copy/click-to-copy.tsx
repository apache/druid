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
import copy from 'copy-to-clipboard';
import React from 'react';

import { AppToaster } from '../../singletons';

export interface ClickToCopyProps {
  text: string;
}

export const ClickToCopy = React.memo(function ClickToCopy(props: ClickToCopyProps) {
  const { text } = props;

  return (
    <a
      className="click-to-copy"
      title={`Click to copy:\n${text}`}
      onClick={() => {
        copy(text, { format: 'text/plain' });
        AppToaster.show({
          message: `'${text}' copied to clipboard`,
          intent: Intent.SUCCESS,
        });
      }}
    >
      {text}
    </a>
  );
});
