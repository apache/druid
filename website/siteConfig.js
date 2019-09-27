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

// See https://docusaurus.io/docs/site-config for all the possible
// site configuration options.

const siteConfig = {
  title: 'Apache Druid',
  tagline: 'A fast analytical database',
  url: 'https://druid.apache.org',
  baseUrl: '/', // Base URL for your project */

  // Used for publishing and more
  projectName: 'ApacheDruid',
  organizationName: 'Apache',

  // For no header links in the top nav bar -> headerLinks: [],
  headerLinks: [
    {href: '/technology', label: 'Technology'},
    {href: '/use-cases', label: 'Use Cases'},
    {href: '/druid-powered', label: 'Powered By'},
    {doc: 'design/index', label: 'Docs'},
    {href: '/community/', label: 'Community'},
    {href: 'https://www.apache.org', label: 'Apache'},
    {href: '/downloads.html', label: 'Download'},
  ],

  /* path to images for header/footer */
  headerIcon: 'img/druid_nav.png',
  footerIcon: 'img/favicon.png',
  favicon: 'img/favicon.png',

  disableHeaderTitle: true,

  /* Colors for website */
  colors: {
    primaryColor: '#1C1C26',
    secondaryColor: '#3b3b50',
  },

  /* Custom fonts for website */
  /*
  fonts: {
    myFont: [
      "Times New Roman",
      "Serif"
    ],
    myOtherFont: [
      "-apple-system",
      "system-ui"
    ]
  },
  */

  // This copyright info is used in /core/Footer.js and blog RSS/Atom feeds.
  copyright: `Copyright © ${new Date().getFullYear()} Apache Software Foundation`,

  highlight: {
    // Highlight.js theme to use for syntax highlighting in code blocks.
    theme: 'default',
  },

  docsSideNavCollapsible: true,

  // Add custom scripts here that would be placed in <script> tags.
  scripts: [
    'https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.4/clipboard.min.js',
    '/js/code-block-buttons.js',
  ],

  stylesheets: [
    'https://use.fontawesome.com/releases/v5.7.2/css/all.css',
    '/css/code-block-buttons.css'
  ],

  // On page navigation for the current documentation page.
  onPageNav: 'separate',
  // No .html extensions for paths.
  cleanUrl: false,

  // Open Graph and Twitter card images.
  ogImage: 'img/druid_nav.png',
  twitterImage: 'img/druid_nav.png',

  gaGtag: true,
  gaTrackingId: 'UA-131010415-1',

 editUrl: 'https://github.com/apache/incubator-druid/edit/master/docs/',

  // Show documentation's last contributor's name.
  // enableUpdateBy: true,

  // Show documentation's last update time.
  // enableUpdateTime: true,

  // You may provide arbitrary config keys to be used as needed by your
  // template. For example, if you need your repo's URL...
  //   repoUrl: 'https://github.com/facebook/test-site',
};

module.exports = siteConfig;
