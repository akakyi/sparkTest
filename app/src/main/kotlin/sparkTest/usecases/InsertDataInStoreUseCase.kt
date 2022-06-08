package sparkTest.usecases

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SparkSession
import sparkTest.entity.TableNameEntity
import java.util.*

class InsertDataInStoreUseCase() {

    private val session: SparkSession

    init {
        session = SparkSession.builder()
            .appName("sparkTest")
            .getOrCreate()
    }

    fun execute() {
        val rows = generateSequence(0) { it + 1 }
            .take(10000)
            .toList()
            .map {
                TableNameEntity(
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString(),
                    it
                )
            }
        val frameToWrite = session.createDataFrame(rows, TableNameEntity::class.java)
        //TODO вынести в конфиги
        frameToWrite.write()
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", "jdbc:postgresql://localhost:5432/sparktest")
            .option("dbtable", "public.table_name")
            .option("user", "postgres")
            .option("password", "postgres")
            .mode(SaveMode.Append)
            .save()
    }

}