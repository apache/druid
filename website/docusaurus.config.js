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
  "onBrokenMarkdownLinks": "log",
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
          "sidebarPath": "../website/sidebars.json"
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
    ]
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
          "href": "/use-cases",
          "label": "Use Cases",
          "position": "right",
          "items": [
            {
              "href": "/druid-powered",
              "label": "Powered By"
            },
          ],
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