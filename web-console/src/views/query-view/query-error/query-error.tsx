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

import React, { useState } from 'react';

import { HighlightText } from '../../../components';
import { DruidError, RowColumn } from '../../../utils';

import './query-error.scss';

export interface QueryErrorProps {
  error: DruidError;
  moveCursorTo: (rowColumn: RowColumn) => void;
  queryString?: string;
  onQueryStringChange?: (newQueryString: string, run?: boolean) => void;
}

export const QueryError = React.memo(function QueryError(props: QueryErrorProps) {
  const { error, moveCursorTo, queryString, onQueryStringChange } = props;
  const [showMode, setShowMore] = useState(false);

  if (!error.errorMessage) {
    return <div className="query-error">{error.message}</div>;
  }

  const { position, suggestion } = error;
  let suggestionElement: JSX.Element | undefined;
  if (suggestion && queryString && onQueryStringChange) {
    const newQuery = suggestion.fn(queryString);
    if (newQuery) {
      suggestionElement = (
        <p>
          Suggestion:{' '}
          <span
            className="suggestion"
            onClick={() => {
              onQueryStringChange(newQuery, true);
            }}
          >
            {suggestion.label}
          </span>
        </p>
      );
    }
  }

  return (
    <div className="query-error">
      {suggestionElement}
      {error.error && <p>{`Error: ${error.error}`}</p>}
      {error.errorMessageWithoutExpectation && (
        <p>
          {position ? (
            <HighlightText
              text={error.errorMessageWithoutExpectation}
              find={position.match}
              replace={
                <span
                  className="cursor-link"
                  onClick={() => {
                    moveCursorTo(position);
                  }}
                >
                  {position.match}
                </span>
              }
            />
          ) : (
            error.errorMessageWithoutExpectation
          )}
          {error.expectation && !showMode && (
            <>
              {' '}
              <span className="more-or-less" onClick={() => setShowMore(true)}>
                More...
              </span>
            </>
          )}
        </p>
      )}
      {error.expectation && showMode && (
        <p>
          {error.expectation}{' '}
          <span className="more-or-less" onClick={() => setShowMore(false)}>
            Less...
          </span>
        </p>
      )}
      {error.errorClass && <p>{error.errorClass}</p>}
    </div>
  );
});
