/*
 * This Kotlin source file was generated by the Gradle 'init' task.
 */
package sparkTest

import sparkTest.usecases.ReadFromStoreAndPushToQueueUseCase

fun main() {
//    val useCase = InsertDataInStoreUseCase()
    val useCase = ReadFromStoreAndPushToQueueUseCase()
    useCase.execute()
}
