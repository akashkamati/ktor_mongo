package com.example.data

import com.example.User
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.types.ObjectId

data class UserEntity(
    @BsonId
    val id:String = ObjectId().toString(),
    val name:String,
    val email:String,
    val profession:String,
    val age:Int,
    val country:String
){
    fun toUser():User{
        return User(
            id = id,
            name = name,
            email = email,
            profession = profession,
            age = age,
            country = country
        )
    }
}
