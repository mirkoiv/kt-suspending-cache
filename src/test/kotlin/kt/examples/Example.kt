package kt.examples

import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kt.suspendingcache.SuspendingCache

class UserApiClient(val cache: SuspendingCache) {

    suspend fun getUser(userId: String): User? {
        return cache.get(userId) {
            println("Fetching user $userId from API...")
            delay(1000) // Simulate API call
            User(userId, "User Name $userId", "user$userId@example.com")
        }
    }
}

data class User(val id: String, val name: String, val email: String)

suspend fun main() = coroutineScope {
    val cache = SuspendingCache()

    val userClient = UserApiClient(cache)

    val user = async {
        println("Requesting user-123")
        userClient.getUser("user-123")
    }.await()

    println("Result: $user")
}
