package com.example.data

class UsersDataSource {

    private val db = MongoDatabaseFactory.db

    private val usersCollection = db.getCollection<UserEntity>("users")


    suspend fun insertOneUser(entity: UserEntity) : Boolean{
        return usersCollection.insertOne(entity).wasAcknowledged()
    }

    suspend fun insertMultipleUsers(entities:List<UserEntity>) :Boolean{
        return usersCollection.insertMany(entities).wasAcknowledged()
    }

}