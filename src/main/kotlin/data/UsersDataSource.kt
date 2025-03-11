package com.example.data

import com.example.User
import com.mongodb.client.model.Accumulators
import com.mongodb.client.model.Aggregates
import com.mongodb.client.model.DeleteOneModel
import com.mongodb.client.model.Filters
import com.mongodb.client.model.InsertOneModel
import com.mongodb.client.model.Projections
import com.mongodb.client.model.ReplaceOneModel
import com.mongodb.client.model.Sorts
import com.mongodb.client.model.UpdateOneModel
import com.mongodb.client.model.UpdateOptions
import com.mongodb.client.model.Updates
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import kotlinx.serialization.Serializable
import org.bson.codecs.pojo.annotations.BsonId
import org.bson.conversions.Bson

data class CountResult(val count:Long)
data class CountryResult(val count:Long,@BsonId val country: String)
data class ProfessionAgeAvgResult(val averageAge:Double,@BsonId val profession: String)

class UsersDataSource {

    private val db = MongoDatabaseFactory.db

    private val usersCollection = db.getCollection<UserEntity>("users")

    suspend fun getTotalCount() : Long{
        val pipeline = listOf(Aggregates.count())
        val result = usersCollection.aggregate<CountResult>(pipeline).firstOrNull()

        return result?.count ?: 0

    }

    suspend fun getAverageAgeByProfession():Map<String,Double>{
        val pipeline = listOf(
            Aggregates.group("\$profession",Accumulators.avg("averageAge","\$age"))
        )

        val result = usersCollection.aggregate<ProfessionAgeAvgResult>(pipeline).toList()

        val responseData = mutableMapOf<String,Double>()

        result.forEach {
            responseData[it.profession] = it.averageAge
        }
        return responseData

    }

    suspend fun getCountryWithUsersCount():Map<String,Long>{

        val pipeline = listOf(
            Aggregates.group("\$country",Accumulators.sum("count",1))
        )

        val result = usersCollection.aggregate<CountryResult>(pipeline).toList()

        val responseData = mutableMapOf<String,Long>()
        result.forEach {
            responseData[it.country] = it.count
        }
        return responseData


    }


    suspend fun bulkOperations() : String{

        val operations = listOf(
            InsertOneModel(
                UserEntity(
                    name = "name",
                    email = "name@ex.com",
                    profession = "profession",
                    age = 10,
                    country = "country"
                )
            ),
            InsertOneModel(
                UserEntity(
                    name = "name1",
                    email = "name1@ex.com",
                    profession = "profession1",
                    age = 10,
                    country = "country1"
                )
            ),
            ReplaceOneModel(
                Filters.eq("_id","67c92f876a110b5dd43bf9cb"),
                UserEntity(
                    id = "67c92f876a110b5dd43bf9cb",
                    name = " replaced name1",
                    email = "replaced@ex.com",
                    profession = "replaced profession1",
                    age = 10,
                    country = "replaced country1"
                )
            ),
            UpdateOneModel(
                Filters.eq("_id","67c92fab6a110b5dd43bf9cd"),
                Updates.set("name","Updated name")
            ),
            DeleteOneModel(
                Filters.eq("_id","67c92fab6a110b5dd43bf9cc")
            )
        )

        val result = usersCollection.bulkWrite(operations)

        return "Inserted: ${result.insertedCount}, Updated: ${result.modifiedCount}, Deleted: ${result.deletedCount}"


    }

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