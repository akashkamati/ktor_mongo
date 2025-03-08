package com.example

import com.example.data.UserEntity
import com.example.data.UsersDataSource
import io.ktor.http.*
import io.ktor.server.application.*
import io.ktor.server.request.*
import io.ktor.server.response.*
import io.ktor.server.routing.*
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json
import java.io.File

fun Application.configureRouting(usersDataSource: UsersDataSource) {
    routing {

        patch("user"){
            val user = call.receive<User>()
            val result = usersDataSource.updateOneUser(user)
            call.respond(mapOf("success" to result))
        }

        put("user"){
            val user = call.receive<User>()
            val result = usersDataSource.replaceUser(user)
            call.respond(mapOf("success" to result))
        }


        patch("users"){
            val age = call.queryParameters["age"]?.toIntOrNull() ?: 20
            val name = call.queryParameters["name"] ?: ""
            val result = usersDataSource.updateMultipleUsers(age,name)
            call.respond(mapOf("success" to result))
        }


        get("users"){
            val id = call.queryParameters["id"] ?: return@get call.respond(HttpStatusCode.BadRequest)
            val user = usersDataSource.getUserById(id) ?: return@get call.respond(HttpStatusCode.NotFound)
            call.respond(user)
        }

        get("users/filter"){
            val age = call.queryParameters["age"]?.toIntOrNull() ?: return@get call.respond(HttpStatusCode.BadRequest)
            val country = call.queryParameters["country"]
            val users = usersDataSource.filterUsers(age,country ?: "")
            call.respond(users)
        }

        get("allUsers") {
            val page = call.queryParameters["page"]?.toIntOrNull() ?: 1
            val users = usersDataSource.getAllUsers(page)
            call.respond(users)
        }



        post("user"){

            val user = call.receive<User>()
            val entity = user.toUserEntity()

            val result = usersDataSource.insertOneUser(entity)
            call.respond(mapOf("success" to result))

        }

        post("users"){
            val users = call.receive<List<User>>()
            val entities = users.map { it.toUserEntity() }
            val result = usersDataSource.insertMultipleUsers(entities)
            call.respond(mapOf("success" to result))
        }

        post("usersFromFile"){

            val path = "dummy_data/users.json"

            val jsonString = File(path).readText()

            val users:List<User> = Json.decodeFromString(jsonString)

            val entities = users.map { it.toUserEntity() }
            val result = usersDataSource.insertMultipleUsers(entities)
            call.respond(mapOf("success" to result))
        }



    }
}

@Serializable
data class User(
    val id:String? = null,
    val name:String,
    val email:String,
    val profession:String,
    val age:Int,
    val country:String
){
    fun toUserEntity():UserEntity{
        return UserEntity(
            name = name,
            email = email,
            profession = profession,
            age = age,
            country = country
        )
    }
}
