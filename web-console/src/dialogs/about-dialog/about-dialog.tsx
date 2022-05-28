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

import { AnchorButton, Button, Classes, Dialog, Intent } from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React from 'react';

import { ExternalLink } from '../../components';
import { getLink } from '../../links';

export interface AboutDialogProps {
  onClose: () => void;
}

export const AboutDialog = React.memo(function AboutDialog(props: AboutDialogProps) {
  const { onClose } = props;

  return (
    <Dialog
      className="about-dialog"
      icon={IconNames.GRAPH}
      onClose={onClose}
      title="Apache Druid"
      isOpen
      canEscapeKeyClose
    >
      <div className={Classes.DIALOG_BODY}>
        <p>
          <strong>Apache Druid is a high performance real-time analytics database.</strong>
        </p>
        <p>
          For help and support with Druid, please refer to the{' '}
          <ExternalLink href={getLink('COMMUNITY')}>community page</ExternalLink> and the{' '}
          <ExternalLink href={getLink('USER_GROUP')}>user groups</ExternalLink>.
        </p>
        <p>
          Druid is made with ❤️ by a community of passionate developers. To contribute, join in the
          discussion on the{' '}
          <ExternalLink href={getLink('DEVELOPER_GROUP')}>developer group</ExternalLink>.
        </p>
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button onClick={onClose}>Close</Button>
          <AnchorButton intent={Intent.PRIMARY} href={getLink('WEBSITE')} target="_blank">
            Visit Druid
          </AnchorButton>
        </div>
      </div>
    </Dialog>
  );
});
