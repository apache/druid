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

import {
  Button,
  Classes,
  Dialog,
  FormGroup,
  InputGroup,
  Intent,
  Menu,
  MenuItem,
  Popover,
  Position,
  Radio,
  RadioGroup,
  TextArea,
} from '@blueprintjs/core';
import { IconNames } from '@blueprintjs/icons';
import React, { useState } from 'react';

import { ExternalLink } from '../../components';
import type { IngestionSpec, QueryWithContext } from '../../druid-models';
import { convertSupervisorToSql } from '../../helpers/supervisor-conversion';
import { Api, AppToaster } from '../../singletons';
import { deepGet, tickIcon } from '../../utils';

import './supervisor-to-sql-dialog.scss';

export interface SupervisorToSqlDialogProps {
  onConvert(converted: QueryWithContext, datasource?: string): void;
  onClose(): void;
}

export const SupervisorToSqlDialog = React.memo(function SupervisorToSqlDialog(
  props: SupervisorToSqlDialogProps,
) {
  const { onConvert, onClose } = props;

  const [supervisorSource, setSupervisorSource] = useState<'select' | 'paste'>('select');
  const [selectedSupervisor, setSelectedSupervisor] = useState<string>('');
  const [pastedSupervisor, setPastedSupervisor] = useState<string>('');
  const [availableSupervisors, setAvailableSupervisors] = useState<string[]>([]);
  const [supervisorSpec, setSupervisorSpec] = useState<IngestionSpec | undefined>();

  const [fileLocation, setFileLocation] = useState<string>('');
  const [fileType, setFileType] = useState<string>('json');

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | undefined>();

  React.useEffect(() => {
    void loadSupervisors();
  }, []);

  async function loadSupervisors() {
    try {
      const supervisors = await Api.instance.get<string[]>('/druid/indexer/v1/supervisor');
      // Don't auto-select; leave the button showing "Select supervisor" until the user picks one
      setAvailableSupervisors(supervisors.data);
    } catch (e) {
      setError(`Failed to load supervisors: ${e.message}`);
    }
  }

  async function loadSupervisorSpec(supervisorId: string) {
    if (!supervisorId) return;

    setLoading(true);
    setError(undefined);

    try {
      const resp = await Api.instance.get<IngestionSpec>(
        `/druid/indexer/v1/supervisor/${Api.encodePath(supervisorId)}`,
      );
      setSupervisorSpec(resp.data);

      // Auto-populate file location from ioConfig if available
      const ioConfig = deepGet(resp.data, 'spec.ioConfig');
      if (ioConfig?.inputSource?.uris) {
        setFileLocation(ioConfig.inputSource.uris[0] || '');
      } else if (ioConfig?.inputSource?.baseDir) {
        setFileLocation(ioConfig.inputSource.baseDir);
      }
    } catch (e) {
      setError(`Failed to load supervisor spec: ${e.message}`);
    } finally {
      setLoading(false);
    }
  }

  function parsePastedSupervisor() {
    if (!pastedSupervisor.trim()) {
      // Clear any previously parsed spec so a blank/cleared paste can't submit a stale supervisor
      setSupervisorSpec(undefined);
      setError(undefined);
      return;
    }

    try {
      const parsed = JSON.parse(pastedSupervisor);
      setSupervisorSpec(parsed);
      setError(undefined);

      // Auto-populate file location from ioConfig if available
      const ioConfig = deepGet(parsed, 'spec.ioConfig');
      if (ioConfig?.inputSource?.uris) {
        setFileLocation(ioConfig.inputSource.uris[0] || '');
      } else if (ioConfig?.inputSource?.baseDir) {
        setFileLocation(ioConfig.inputSource.baseDir);
      }
    } catch (e) {
      setError(`Invalid JSON: ${e.message}`);
      setSupervisorSpec(undefined);
    }
  }

  function handleConvert() {
    if (!supervisorSpec) {
      AppToaster.show({
        message: 'No supervisor spec loaded',
        intent: Intent.DANGER,
      });
      return;
    }

    if (!fileLocation) {
      AppToaster.show({
        message: 'Please specify a file location',
        intent: Intent.DANGER,
      });
      return;
    }

    let converted: QueryWithContext;
    try {
      converted = convertSupervisorToSql(supervisorSpec, {
        fileLocation,
        fileType,
      });
    } catch (e) {
      AppToaster.show({
        message: `Could not convert supervisor: ${e.message}`,
        intent: Intent.DANGER,
      });
      return;
    }

    AppToaster.show({
      message: 'Supervisor converted to SQL, please review',
      intent: Intent.SUCCESS,
    });

    onConvert(converted, deepGet(supervisorSpec, 'spec.dataSchema.dataSource'));
  }

  React.useEffect(() => {
    if (supervisorSource !== 'select') return;
    if (selectedSupervisor) {
      void loadSupervisorSpec(selectedSupervisor);
    } else {
      // No supervisor selected (e.g. none available); don't keep a spec from paste mode around
      setSupervisorSpec(undefined);
    }
  }, [selectedSupervisor, supervisorSource]);

  React.useEffect(() => {
    if (supervisorSource !== 'paste') return;
    // Always reparse on entering paste mode or editing the text so a stale select-mode spec is
    // dropped and a cleared paste disables Generate SQL
    parsePastedSupervisor();
  }, [pastedSupervisor, supervisorSource]);

  return (
    <Dialog
      className="supervisor-to-sql-dialog"
      isOpen
      onClose={onClose}
      title="Convert supervisor to SQL"
      canOutsideClickClose={false}
    >
      <div className={Classes.DIALOG_BODY}>
        <p>
          Convert a streaming supervisor specification into an{' '}
          <ExternalLink href="https://druid.apache.org/docs/latest/multi-stage-query/">
            MSQ (Multi-Stage Query)
          </ExternalLink>{' '}
          ingestion SQL statement. This generates a one-time batch ingestion that reads the supplied
          files — it does not start a streaming ingestion and will not continuously ingest new data.
        </p>

        <FormGroup label="Supervisor source">
          <RadioGroup
            selectedValue={supervisorSource}
            onChange={e => setSupervisorSource(e.currentTarget.value as 'select' | 'paste')}
          >
            <Radio label="Select existing supervisor" value="select" />
            <Radio label="Paste supervisor JSON" value="paste" />
          </RadioGroup>
        </FormGroup>

        {supervisorSource === 'select' ? (
          <FormGroup label="Select supervisor">
            <Popover
              position={Position.BOTTOM_LEFT}
              disabled={!availableSupervisors.length}
              content={
                <Menu>
                  {availableSupervisors.map(name => (
                    <MenuItem
                      key={name}
                      icon={tickIcon(name === selectedSupervisor)}
                      text={name}
                      onClick={() => setSelectedSupervisor(name)}
                    />
                  ))}
                </Menu>
              }
            >
              <Button
                text={selectedSupervisor || 'Select supervisor'}
                rightIcon={IconNames.CARET_DOWN}
                disabled={!availableSupervisors.length}
              />
            </Popover>
          </FormGroup>
        ) : (
          <FormGroup
            label="Supervisor JSON"
            helperText="Paste the complete supervisor specification"
          >
            <TextArea
              value={pastedSupervisor}
              onChange={e => setPastedSupervisor(e.target.value)}
              fill
              rows={10}
              placeholder='{"type": "kafka", "spec": {...}}'
            />
          </FormGroup>
        )}

        <FormGroup
          label="File location"
          helperText="S3 URI, local path, or other supported input source"
        >
          <InputGroup
            value={fileLocation}
            onChange={e => setFileLocation(e.target.value)}
            placeholder="s3://my-bucket/path/to/files/"
            fill
          />
        </FormGroup>

        <FormGroup label="File type">
          <RadioGroup
            selectedValue={fileType}
            onChange={e => setFileType(e.currentTarget.value)}
            inline
          >
            <Radio label="JSON" value="json" />
            <Radio label="CSV" value="csv" />
            <Radio label="Parquet" value="parquet" />
            <Radio label="ORC" value="orc" />
          </RadioGroup>
        </FormGroup>

        {error && (
          <FormGroup>
            <div className="error-message">{error}</div>
          </FormGroup>
        )}

        {!supervisorSpec && !loading && (
          <FormGroup>
            <div style={{ color: '#999', fontSize: '12px', fontStyle: 'italic' }}>
              {supervisorSource === 'select'
                ? 'Select a supervisor to continue...'
                : 'Paste a supervisor JSON to continue...'}
            </div>
          </FormGroup>
        )}
      </div>
      <div className={Classes.DIALOG_FOOTER}>
        <div className={Classes.DIALOG_FOOTER_ACTIONS}>
          <Button text="Close" onClick={onClose} />
          <Button
            text="Generate SQL"
            intent={Intent.PRIMARY}
            onClick={handleConvert}
            disabled={!supervisorSpec || !fileLocation || loading}
            icon={IconNames.CODE}
          />
        </div>
      </div>
    </Dialog>
  );
});
