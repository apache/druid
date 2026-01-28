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

import { Button, Intent, Popover, Position } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { type JSX } from 'react';

import { Capabilities } from '../../../helpers';
import { getLink } from '../../../links';
import { ExternalLink } from '../../external-link/external-link';
import { PopoverText } from '../../popover-text/popover-text';

export interface RestrictedModeProps {
  capabilities: Capabilities;
  onUnrestrict(capabilities: Capabilities): void;
  onUseAutomaticCapabilityDetection?: () => void;
}

export const RestrictedMode = React.memo(function RestrictedMode(props: RestrictedModeProps) {
  const { capabilities, onUnrestrict, onUseAutomaticCapabilityDetection } = props;
  const mode = capabilities.getModeExtended();

  let label: string;
  let message: JSX.Element;
  switch (mode) {
    case 'full':
      return null; // Do not show anything

    case 'no-sql':
      label = 'No SQL mode';
      message = (
        <p>
          The SQL endpoint is disabled. The console will fall back to{' '}
          <ExternalLink href={getLink('DOCS_API')}>native Druid APIs</ExternalLink> and will operate
          with limited functionality. Refer to{' '}
          <ExternalLink href={getLink('DOCS_SQL')}>the SQL docs</ExternalLink> for instructions to
          enable the SQL endpoint.
        </p>
      );
      break;

    case 'no-proxy':
      label = 'No management proxy mode';
      message = (
        <p>
          The management proxy is disabled, the console will operate with limited functionality.
        </p>
      );
      break;

    case 'no-sql-no-proxy':
      label = 'No SQL mode';
      message = (
        <p>
          The SQL endpoint and management proxy are disabled. You can only use the console to make
          JSON-based queries.
        </p>
      );
      break;

    case 'coordinator-overlord':
      label = 'Coordinator/Overlord mode';
      message = (
        <p>
          You are accessing the console on the Coordinator/Overlord shared service. Because this
          service lacks access to some APIs, the console will operate in a limited mode. You can
          access the unrestricted version of the console on the Router service.
        </p>
      );
      break;

    case 'coordinator':
      label = 'Coordinator mode';
      message = (
        <p>
          You are accessing the console on the Coordinator service. Because this service lacks
          access to some APIs, the console will operate in a limited mode. You can access the
          unrestricted version of the console on the Router service.
        </p>
      );
      break;

    case 'overlord':
      label = 'Overlord mode';
      message = (
        <p>
          You are accessing the console on the Overlord service. Because this service lacks access
          to some APIs, the console will operate in a limited mode. You can access the unrestricted
          version of the console on the Router service.
        </p>
      );
      break;

    default:
      label = 'Restricted mode';
      message = (
        <p>
          Due to the lack of access to some APIs on this service, the console will operate in a
          limited mode. You can access the unrestricted version of the console on the Router
          service.
        </p>
      );
      break;
  }

  return (
    <Popover
      className="restricted-mode"
      content={
        <PopoverText>
          <p>The console is running in restricted mode.</p>
          {message}
          <p>
            For more info refer to the{' '}
            <ExternalLink href={`${getLink('DOCS')}/operations/web-console`}>
              web console documentation
            </ExternalLink>
            .
          </p>
          {onUseAutomaticCapabilityDetection ? (
            <>
              <p>
                The console did no perform its automatic capability detection because it is running
                in manual capability detection mode.
              </p>
              <p>
                <Button
                  text="Use to automatic capability detection"
                  onClick={onUseAutomaticCapabilityDetection}
                  intent={Intent.PRIMARY}
                />
              </p>
            </>
          ) : (
            <>
              <p>
                It is possible that the console is experiencing an issue with the capability
                detection. You can enable the unrestricted console, but certain features might not
                work if the underlying APIs are not available.
              </p>
              <p>
                <Button
                  icon={IconNames.WARNING_SIGN}
                  text={`Temporarily unrestrict console${
                    capabilities.hasSql() ? '' : ' (with SQL)'
                  }`}
                  onClick={() => onUnrestrict(Capabilities.FULL)}
                />
              </p>
            </>
          )}
          {!capabilities.hasSql() && (
            <p>
              <Button
                icon={IconNames.WARNING_SIGN}
                text="Temporarily unrestrict console (without SQL)"
                onClick={() => onUnrestrict(Capabilities.NO_SQL)}
              />
            </p>
          )}
        </PopoverText>
      }
      position={Position.BOTTOM_RIGHT}
    >
      <Button icon={IconNames.WARNING_SIGN} text={label} intent={Intent.WARNING} minimal />
    </Popover>
  );
});
