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

import { Button, Icon, IconName, Intent } from '@blueprintjs/core';
import classNames from 'classnames';
import React, { ReactNode } from 'react';

import { filterMap } from '../../utils';

import './fancy-tab-pane.scss';

export interface FancyTabButton {
  id: string;
  icon: IconName;
  label: string;
}

interface FancyTabPaneProps {
  className?: string;
  tabs: (FancyTabButton | false | undefined)[];
  activeTab: string;
  onActivateTab(newActiveTab: string): void;
  children?: ReactNode;
}

export const FancyTabPane = React.memo(function FancyTabPane(props: FancyTabPaneProps) {
  const { className, tabs, activeTab, onActivateTab, children } = props;

  return (
    <div className={classNames('fancy-tab-pane', className)}>
      <div className="side-bar">
        {filterMap(tabs, d => {
          if (!d) return;
          return (
            <Button
              className="tab-button"
              icon={<Icon icon={d.icon} size={20} />}
              key={d.id}
              text={d.label}
              intent={activeTab === d.id ? Intent.PRIMARY : Intent.NONE}
              minimal={activeTab !== d.id}
              onClick={() => onActivateTab(d.id)}
            />
          );
        })}
      </div>
      <div className="main-section">{children}</div>
    </div>
  );
});
