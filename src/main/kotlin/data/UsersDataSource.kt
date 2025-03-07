package com.example.data

import com.example.User
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Projections
import com.mongodb.client.model.Sorts
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.serialization.Serializable

class UsersDataSource {

    private val db = MongoDatabaseFactory.db

    private val usersCollection = db.getCollection<UserEntity>("users")

    suspend fun getUserById(id:String):User?{
        val filter = Filters.eq("_id",id)
        val result = usersCollection.find(filter).firstOrNull()
        return result?.toUser()
    }

    fun filterUsers(age:Int,country:String) : Flow<User>{
//        age <= age || country == country
        val filter = Filters.or(
            Filters.lte("age",age),
            Filters.eq("country",country)
        )
        val result = usersCollection.find(filter).map { it.toUser() }
        return result
    }

    fun getAllUsers(page:Int) : Flow<UserResult>{
        val filter = Filters.empty()

        val projections = Projections.fields(
            Projections.include("name","age"),
            Projections.excludeId()
        )

        val pageSize = 10
        val skip = (page -1) * pageSize

        val result = usersCollection.withDocumentClass<UserResult>()
            .find(filter)
            .projection(projections)
            .sort(Sorts.ascending("age"))
            .limit(pageSize)
            .skip(skip)


        return result
    }

    suspend fun insertOneUser(entity: UserEntity) : Boolean{
        return usersCollection.insertOne(entity).wasAcknowledged()
    }

    suspend fun insertMultipleUsers(entities:List<UserEntity>) :Boolean{
        return usersCollection.insertMany(entities).wasAcknowledged()
    }

}

@Serializable
data class UserResult(
    val name:String,
    val age:Int
)