package com.example.data

import com.example.User
import com.mongodb.client.model.Filters
import com.mongodb.client.model.Projections
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.serialization.Serializable
import org.bson.conversions.Bson

class UsersDataSource {

    private val db = MongoDatabaseFactory.db

    private val usersCollection = db.getCollection<UserEntity>("users")


    suspend fun deleteOneUser(id:String) : Long{
        val filter = Filters.eq("_id",id)
        val result = usersCollection.deleteOne(filter)
        return result.deletedCount
    }

    suspend fun deleteMultipleUsers(age: Int) : Long{
        val filter = Filters.gt("age",age)
        val result = usersCollection.deleteMany(filter)
        return result.deletedCount
    }

    suspend fun updateOneUser(user: User) : Boolean{
        val filter = Filters.eq("_id",user.id)

        val updateList = mutableListOf<Bson>().apply {
            if (user.age > 0) add(Updates.set("age",user.age))
            if (user.name.isNotBlank()) add(Updates.set("name",user.name))
            if (user.profession.isNotBlank()) add(Updates.set("profession",user.profession))
            if (user.country.isNotBlank()) add(Updates.set("country",user.country))
            if (user.email.isNotBlank()) add(Updates.set("email",user.email))
        }

        val options = UpdateOptions().upsert(true)

        val updates = Updates.combine(updateList)
        val result = usersCollection.updateOne(filter,updates,options).wasAcknowledged()
        return result
    }


    suspend fun updateMultipleUsers(age: Int,name: String):Boolean{
        val filter = Filters.gt("age",age)

        val updates = Updates.set("name",name)

        val result = usersCollection.updateMany(filter,updates).wasAcknowledged()

        return result

    }

    suspend fun replaceUser(user: User) : Boolean{
        val filter = Filters.eq("_id",user.id)
        val entity = UserEntity(
            id = user.id!!,
            name = user.name,
            age = user.age,
            country = user.country,
            profession = user.profession,
            email = user.email
        )
        val result = usersCollection.replaceOne(filter,entity).wasAcknowledged()
        return result
    }




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