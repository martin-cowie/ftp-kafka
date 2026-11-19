# FTP-Kafka Bridge

An FTP server whose clients publish files to Kafka topics. It holds no user accounts of its own: Kafka decides who may log in.

## Language

**Kafka credentials**:
The username and password a client supplies at FTP login, forwarded to the Kafka cluster as its SASL credentials.
_Avoid_: FTP user account, local user, local credentials

**Login**:
An FTP client's attempt to authenticate, accepted only if the Kafka cluster accepts the client's Kafka credentials. On a cluster without authentication, any Kafka credentials are accepted so long as the cluster is reachable.
_Avoid_: Sign-in, local authentication

**Session**:
One logged-in FTP connection, which publishes to Kafka as the identity established at Login.
_Avoid_: Connection (ambiguous with the Kafka connection)

**Session topic**:
The Kafka topic `SITE SEND` publishes to when given no `--topic`. It starts as `default_topic`, is changed by `SITE TOPIC <topic>`, and lasts only for the Session.
_Avoid_: Current topic, selected topic

**Security protocol**:
How the bridge talks to the Kafka cluster: `PLAINTEXT` (no authentication) or `SASL_PLAINTEXT` (SASL authentication, no encryption). Encrypted protocols are out of scope for now.
_Avoid_: Auth mode
