package com.example

import com.example.data.UsersDataSource
import io.ktor.server.application.*

fun main(args: Array<String>) {
    io.ktor.server.netty.EngineMain.main(args)
}

fun Application.module() {

    val usersDataSource = UsersDataSource()

    configureSerialization()
    configureRouting(usersDataSource)
}
