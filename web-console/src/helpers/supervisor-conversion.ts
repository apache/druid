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

import type { IngestionSpec, InputFormat, InputSource, QueryWithContext } from '../druid-models';
import { deepGet } from '../utils';

import { convertSpecToSql } from './spec-conversion';

export interface SupervisorConversionOptions {
  fileLocation: string;
  fileType: string;
}

function fileLocationToInputSource(fileLocation: string, fileType: string): InputSource {
  if (fileLocation.startsWith('s3://')) {
    const inputSource: InputSource = { type: 's3', uris: [fileLocation] };
    // Add an objectGlob when pointing at a directory so only the chosen file type is read
    if (fileLocation.endsWith('/')) {
      inputSource.objectGlob = `**.${fileType}`;
    }
    return inputSource;
  }

  if (fileLocation.startsWith('gs://')) {
    return { type: 'google', uris: [fileLocation] };
  }

  if (fileLocation.startsWith('http://') || fileLocation.startsWith('https://')) {
    return { type: 'http', uris: [fileLocation] };
  }

  // Default to local for file:// or absolute paths
  return { type: 'local', baseDir: fileLocation.replace('file://', ''), filter: `*.${fileType}` };
}

/**
 * Converts a streaming supervisor spec to an MSQ ingestion SQL statement.
 *
 * A supervisor's dataSchema is identical in shape to a batch ingestion spec's; only the ioConfig
 * differs (a stream rather than files). So rather than reimplement the conversion, we rewrite the
 * supervisor into an `index_parallel` spec that reads from the chosen files and delegate to the
 * existing {@link convertSpecToSql}. This reuses all of its column-type, timestamp, granularity, and
 * metric aggregation handling.
 */
export function convertSupervisorToSql(
  supervisorSpec: IngestionSpec,
  options: SupervisorConversionOptions,
): QueryWithContext {
  const { fileLocation, fileType } = options;

  const dataSchema = deepGet(supervisorSpec, 'spec.dataSchema');
  if (!dataSchema) {
    throw new Error('Supervisor spec missing dataSchema');
  }

  const inputSource = fileLocationToInputSource(fileLocation, fileType);

  // Preserve the supervisor's inputFormat settings (e.g. flattenSpec, CSV columns/header), overriding
  // only the type with the file type chosen for the backfill files.
  const supervisorInputFormat: InputFormat | undefined = deepGet(
    supervisorSpec,
    'spec.ioConfig.inputFormat',
  );
  const inputFormat: InputFormat = supervisorInputFormat
    ? { ...supervisorInputFormat, type: fileType }
    : { type: fileType };

  // convertSpecToSql requires a segment granularity; default to DAY when the supervisor omits one.
  const granularitySpec = { segmentGranularity: 'DAY', ...dataSchema.granularitySpec };

  // Cluster by the leading dimensions, approximating the partitioning a supervisor would produce.
  const dimensionNames: string[] = (deepGet(dataSchema, 'dimensionsSpec.dimensions') || []).map(
    (d: any) => (typeof d === 'string' ? d : d.name),
  );

  const batchSpec: IngestionSpec = {
    type: 'index_parallel',
    spec: {
      dataSchema: { ...dataSchema, granularitySpec },
      ioConfig: { type: 'index_parallel', inputSource, inputFormat },
      tuningConfig: {
        type: 'index_parallel',
        ...(dimensionNames.length
          ? { partitionsSpec: { type: 'range', partitionDimensions: dimensionNames.slice(0, 2) } }
          : {}),
      },
    },
  };

  return convertSpecToSql(batchSpec);
}
