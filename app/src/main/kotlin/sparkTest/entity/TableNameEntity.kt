package sparkTest.entity

import java.io.Serializable

data class TableNameEntity(
    //TODO сюда UUID
    val id: String,
    val col1: String,
    val col2: Int
) : Serializable
