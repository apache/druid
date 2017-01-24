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
|`druid.authentication.kerberos.enabled`|true/false||Must be set to 'true' to enable kerberos authetication.|false|Yes|
|`druid.hadoop.security.kerberos.principal`|`druid@EXAMPLE.COM`| Principal user name, used for internal node communication|empty|Yes|
|`druid.hadoop.security.kerberos.keytab`|`/etc/security/keytabs/druid.headlessUser.keytab`|Path to keytab file used for internal node communication|empty|Yes|
|`druid.hadoop.security.spnego.principal`|`HTTP/_HOST@EXAMPLE.COM`| SPNego service principal used by druid nodes|empty|Yes|
|`druid.hadoop.security.spnego.keytab`|`/etc/security/keytabs/spnego.service.keytab`|SPNego service keytab|empty|Yes|
|`druid.hadoop.security.spnego.authToLocal`||It allows you to set a general rule for mapping principal names to local user names. It will be used if there is not an explicit mapping for the principal name that is being translated.|DEFAULT|No|
|`druid.hadoop.security.spnego.excludedPaths`|`['/status','/health']`| Array of HTTP paths which which does NOT need to be authenticated.|\["/status"]|No|

As a note, it is required that the SPNego principal in use by the druid nodes must start with HTTP (This specified by [RFC-4559](https://tools.ietf.org/html/rfc4559)) and must be of the form "HTTP/_HOST@REALM". 
The special string _HOST will be replaced automatically with the value of config `druid.host`




