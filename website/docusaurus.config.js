module.exports={
  "title": "Apache Druid",
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
  "onBrokenMarkdownLinks": "log",
  "presets": [
    [
      "@docusaurus/preset-classic",
      {
        "docs": {
          "showLastUpdateAuthor": true,
          "showLastUpdateTime": true,
          "editUrl": "https://github.com/apache/druid/edit/master/docs/",
          "path": "../docs",
          "sidebarPath": "../website/sidebars.json"
        },
        "blog": {},
        "theme": {
          "customCss": "../src/css/customTheme.css"
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
        ]
      }
    ]
  ],
  "themeConfig": {
    "navbar": {
      "title": "Apache Druid",
      "logo": {
        "src": "img/druid_nav.png"
      },
      "items": [
        {
          "href": "/technology",
          "label": "Technology",
          "position": "left"
        },
        {
          "href": "/use-cases",
          "label": "Use Cases",
          "position": "left"
        },
        {
          "href": "/druid-powered",
          "label": "Powered By",
          "position": "left"
        },
        {
          "to": "docs/design/",
          "label": "Docs",
          "position": "left"
        },
        {
          "href": "/community/",
          "label": "Community",
          "position": "left"
        },
        {
          "href": "https://www.apache.org",
          "label": "Apache",
          "position": "left"
        },
        {
          "href": "/downloads.html",
          "label": "Download",
          "position": "left"
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
    "algolia": {
      "appId": "CPK9PMSCEY",
      "apiKey": "d4ef4ffe3a2f0c7d1e34b062fd98736b",
      "indexName": "apache_druid",
      "algoliaOptions": {
        "facetFilters": [
          "language:LANGUAGE",
          "version:druidVersion"
        ]
      }
    }
  }
}