---
layout: doc_page
---

# Druid-Kerberos

Druid Extension to enable Authentication for Druid Nodes using Kerberos.
This extension adds AuthenticationFilter which is used to protect HTTP Endpoints using the simple and protected GSSAPI negotiation mechanism [SPNEGO](https://en.wikipedia.org/wiki/SPNEGO). 
Make sure to [include](../../operations/including-extensions.html) `druid-kerberos` as an extension.


## Configuration

|Property|Possible Values|Description|Default|required|
|--------|---------------|-----------|-------|--------|
|`druid.hadoop.security.kerberos.principal`|`druid@EXAMPLE.COM`| Principal user name, used for internal node communication|empty|Yes|
|`druid.hadoop.security.kerberos.keytab`|`/etc/security/keytabs/druid.keytab`|Path to keytab file used for internal node communication|empty|Yes|
|`druid.hadoop.security.spnego.principal`|`HTTP/_HOST@EXAMPLE.COM`| SPNego service principal used by druid nodes|empty|Yes|
|`druid.hadoop.security.spnego.keytab`|`/etc/security/keytabs/spnego.service.keytab`|SPNego service keytab used by druid nodes|empty|Yes|
|`druid.hadoop.security.spnego.authToLocal`|`RULE:[1:$1@$0](druid@EXAMPLE.COM)s/.*/druid DEFAULT`|It allows you to set a general rule for mapping principal names to local user names. It will be used if there is not an explicit mapping for the principal name that is being translated.|DEFAULT|No|
|`druid.hadoop.security.spnego.excludedPaths`|`['/status','/health']`| Array of HTTP paths which which does NOT need to be authenticated.|None|No|
|`druid.hadoop.security.spnego.cookieSignatureSecret`|`secretString`| Secret used to sign authentication cookies. It is advisable to explicitly set it, if you have multiple druid ndoes running on same machine with different ports as the Cookie Specification does not guarantee isolation by port.|<Random value>|No|

As a note, it is required that the SPNego principal in use by the druid nodes must start with HTTP (This specified by [RFC-4559](https://tools.ietf.org/html/rfc4559)) and must be of the form "HTTP/_HOST@REALM". 
The special string _HOST will be replaced automatically with the value of config `druid.host`

### Auth to Local Syntax


`druid.hadoop.security.spnego.authToLocal` allows you to set a general rules for mapping principal names to local user names.
The syntax for mapping rules is `RULE:\[n:string](regexp)s/pattern/replacement/g`. The integer n indicates how many components the target principal should have. If this matches, then a string will be formed from string, substituting the realm of the principal for $0 and the n‘th component of the principal for $n. e.g. if the principal was druid/admin then `\[2:$2$1suffix]` would result in the string `admindruidsuffix`.
If this string matches regexp, then the s//\[g] substitution command will be run over the string. The optional g will cause the substitution to be global over the string, instead of replacing only the first match in the string.
If required, multiple rules can be be joined by newline character and specified as a String. 

## Accessing Druid HTTP end points when kerberos security is enabled 
1. To access druid HTTP endpoints via curl user will need to first login using `kinit` command as follows -  

    ```
    kinit -k -t <path_to_keytab_file> user@REALM.COM
    ```

2. Once the login is successful verify that login is successful using `klist` command
3. Now you can access druid HTTP endpoints using curl command as follows - 

    ```
    curl --negotiate -u:anyUser -b ~/cookies.txt -c ~/cookies.txt -X POST -H'Content-Type: application/json' <HTTP_END_POINT>
    ```

    e.g to send a query from file `query.json` to druid broker use this command -

    ```
    curl --negotiate -u:anyUser -b ~/cookies.txt -c ~/cookies.txt -X POST -H'Content-Type: application/json'  http://broker-host:port/druid/v2/?pretty -d @query.json
    ```
    Note: Above command will authenticate the user first time using SPNego negotiate mechanism and store the authentication cookie in file. For subsequent requests the cookie will be used for authentication.

## Accessing coordinator or overlord console from web browser
To access Coordinator/Overlord console from browser you will need to configure your browser for SPNego authentication as follows - 

1. Safari - No configurations required.
2. Firefox - Open firefox and follow these steps - 
    1. Go to `about:config` and search for `network.negotiate-auth.trusted-uris`.
    2. Double-click and add the following values: `"http://druid-coordinator-hostname:ui-port"` and `"http://druid-overlord-hostname:port"`
3. Google Chrome - From the command line run following commands - 
    1. `google-chrome --auth-server-whitelist="druid-coordinator-hostname" --auth-negotiate-delegate-whitelist="druid-coordinator-hostname"`
    2. `google-chrome --auth-server-whitelist="druid-overlord-hostname" --auth-negotiate-delegate-whitelist="druid-overlord-hostname"`
4. Internet Explorer -
    1. Configure trusted websites to include `"druid-coordinator-hostname"` and `"druid-overlord-hostname"`
    2. Allow negotiation for the UI website.

## Sending Queries programmatically
Many HTTP client libraries, such as Apache Commons [HttpComponents](https://hc.apache.org/), already have support for performing SPNEGO authentication. You can use any of the available HTTP client library to communicate with druid cluster. 
