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

// This is set to the latest available version and should be updated to the next version before release
const DRUID_DOCS_VERSION = 'latest';

function fillVersion(str: string): string {
  return str.replace(/\{\{VERSION}}/g, DRUID_DOCS_VERSION);
}

export interface Links {
  websiteHref: string;
  githubHref: string;
  docsHref: string;
  communityHref: string;
  slackHref: string;
  userGroup: string;
  developerGroup: string;
}

const DEFAULT_LINKS: Links = {
  websiteHref: 'https://druid.apache.org',
  githubHref: 'https://github.com/apache/druid',
  docsHref: fillVersion('https://druid.apache.org/docs/{{VERSION}}'),
  communityHref: 'https://druid.apache.org/community/',
  slackHref: 'https://druid.apache.org/community/join-slack',
  userGroup: 'https://groups.google.com/forum/#!forum/druid-user',
  developerGroup: 'https://lists.apache.org/list.html?dev@druid.apache.org',
};

const links = DEFAULT_LINKS;

export function setLinkOverrides(linkOverrides: Links) {
  const keys = Object.keys(DEFAULT_LINKS) as (keyof Links)[];
  for (const k of keys) {
    if (Object.hasOwn(linkOverrides, k)) {
      links[k] = fillVersion(String(linkOverrides[k]));
    }
  }
}

export type LinkNames =
  | 'WEBSITE'
  | 'GITHUB'
  | 'DOCS'
  | 'DOCS_SQL'
  | 'DOCS_RUNE'
  | 'DOCS_API'
  | 'DOCS_MSQ_ERROR'
  | 'COMMUNITY'
  | 'SLACK'
  | 'USER_GROUP'
  | 'DEVELOPER_GROUP';

export function getLink(linkName: LinkNames): string {
  switch (linkName) {
    case 'WEBSITE':
      return links.websiteHref;
    case 'GITHUB':
      return links.githubHref;
    case 'DOCS':
      return links.docsHref;
    case 'DOCS_SQL':
      return `${links.docsHref}/querying/sql.html`;
    case 'DOCS_RUNE':
      return `${links.docsHref}/querying/querying.html`;
    case 'DOCS_API':
      return `${links.docsHref}/api-reference/api-reference.html`;
    case 'DOCS_MSQ_ERROR':
      return `${links.docsHref}/multi-stage-query/reference.html`;
    case 'COMMUNITY':
      return links.communityHref;
    case 'SLACK':
      return links.slackHref;
    case 'USER_GROUP':
      return links.userGroup;
    case 'DEVELOPER_GROUP':
      return links.developerGroup;
  }
}
