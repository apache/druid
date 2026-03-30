package app.druid
import rego.v1
default allow := false
allow if {
	user_is_admin
}
allow if {
	some grant
	user_is_granted[grant]
	input.action == grant.action
	regex.match(grant.resource.name, input.resource.name)
	input.resource.type == grant.resource.type
}
user_is_admin if {
	"admin" in data.user_roles[input.authenticationResult.identity]
}
user_is_granted contains grant if {
	some role in data.user_roles[input.authenticationResult.identity]
	some grant in data.role_grants[role]
}
