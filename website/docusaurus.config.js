/*
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
  */

const Redirects = require('./redirects.js').Redirects;

module.exports={
  "title": "Apache® Druid",
  "tagline": "A fast analytical database",
  "url": "https://druid.apache.org",
  "baseUrl": "/",
  
  "organizationName": "Apache",
  "projectName": "ApacheDruid",
  "scripts": [
    "https://cdnjs.cloudflare.com/ajax/libs/clipboard.js/2.0.4/clipboard.min.js",
    "/js/code-block-buttons.js"
  ],
  "stylesheets": [
    "https://use.fontawesome.com/releases/v5.7.2/css/all.css",
    "/css/code-block-buttons.css"
  ],
  "favicon": "img/favicon.png",
  "customFields": {
    "disableHeaderTitle": true
  },
  "onBrokenLinks": "log",
  "onBrokenMarkdownLinks": "throw",
  markdown: {
    mermaid: true,
  },
  "presets": [
    [
      "@docusaurus/preset-classic",
      {
        "docs": {
          "showLastUpdateAuthor": false,
          "showLastUpdateTime": false,
          "editUrl": "https://github.com/apache/druid/edit/master/docs/",
          "path": "../docs",
          "routeBasePath": "/docs/latest",
          "sidebarPath": "sidebars.json"
        },
        "blog": {},
        "theme": {
          "customCss": "./src/css/customTheme.css"
        },
        "gtag": {
          "trackingID": "UA-131010415-1"
        }
      }
    ]
  ],
  "plugins": [
    [
      "@docusaurus/plugin-client-redirects",
      {
        "fromExtensions": [
          "html"
        ],
        "redirects": Redirects
      }
    ],
    "docusaurus-lunr-search",
    "@docusaurus/theme-mermaid"
  ],
  "themeConfig": {
    colorMode: {
      defaultMode: 'light',
      disableSwitch: true,
      respectPrefersColorScheme: false,
    },
    "navbar": {
      "logo": {
        "src": "img/druid_nav.png"
      },
      "items": [
        {
          "href": "/technology",
          "label": "Technology",
          "position": "right"
        },
        {
          "href": "/druid-powered",
          "label": "Powered By",
          "position": "right",
        },        
        {
          "href": "/use-cases",
          "label": "Use Cases",
          "position": "right",
        },
        {
          "to": "docs/latest/design/",
          "label": "Docs",
          "position": "right"
        },
        {
          "href": "/community/",
          "label": "Community",
          "position": "right"
        },
        {
          "href": "https://www.apache.org",
          "label": "Apache",
          "position": "right",
          "items": [
            {
              "href": "https://www.apache.org/",
              "label": "Foundation"
            },
            {
              "href": "https://apachecon.com/?ref=druid.apache.org",
              "label": "Events"
            },
            {
              "href": "https://www.apache.org/licenses/",
              "label": "License"
            },
            {
              "href": "https://www.apache.org/foundation/thanks.html",
              "label": "Thanks"
            },
            {
              "href": "https://www.apache.org/security/",
              "label": "Security"
            },
            {
              "href": "https://www.apache.org/foundation/sponsorship.html",
              "label": "Sponsorship"
            }


          ],
        },
        {
          "href": "/downloads/",
          "label": "Download",
          "position": "right"
        }
      ]
    },
    "image": "img/druid_nav.png",
    "footer": {
      "links": [],
      "copyright": "Copyright © 2023 Apache Software Foundation",
      "logo": {
        "src": "img/favicon.png"
      }
    },
  }
}