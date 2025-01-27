package com.kostynenko.tools.pgwebjdbc

import com.fasterxml.jackson.annotation.JsonAlias


class PgwebConnectionResponse (
    val currentDatabase: String,
    val currentSchemas: String,
    val currentUser: String,
    @JsonAlias("inet_client_addr")
    val inetClientAddress: String,
    val inetClientPort: Long,
    @JsonAlias("inet_server_addr")
    val inetServerAddress: String,
    val inetServerPort: Long,
    val sessionUser: String,
    val version: String
)
