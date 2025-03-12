package com.example

import com.example.data.UsersDataSource
import io.ktor.server.application.*
import kotlinx.coroutines.runBlocking

fun main(args: Array<String>) {
    io.ktor.server.netty.EngineMain.main(args)
}

fun Application.module() {

    val usersDataSource = UsersDataSource()

    runBlocking {
        usersDataSource.createUserTextIndex()
    }

    configureSerialization()
    configureRouting(usersDataSource)
}
