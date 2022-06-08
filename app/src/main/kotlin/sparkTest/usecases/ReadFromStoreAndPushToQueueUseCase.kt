package sparkTest.usecases

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.SparkSession
import sparkTest.entity.TableNameEntity
import java.io.Serializable

class ReadFromStoreAndPushToQueueUseCase : Serializable {

    private val session: SparkSession

    private val mappper: ObjectMapper

    init {
        session = SparkSession.builder()
            .appName("sparkTest")
            .getOrCreate()
        mappper = ObjectMapper()
    }

    fun execute() {
        //TODO вынести в конфиги
        val dataframe = session.read()
            .format("jdbc")
            .option("driver", "org.postgresql.Driver")
            .option("url", "jdbc:postgresql://localhost:5432/sparktest")
            .option("dbtable", "public.table_name")
            .option("user", "postgres")
            .option("password", "postgres")
            .load()
            .`as`(Encoders.bean(TableNameEntity::class.java))

        val jsonsRdd = dataframe
            .javaRDD()
            .map {
                mappper.writeValueAsString(it)
            }

        session.createDataFrame(jsonsRdd, String::class.java)
            .write()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("topic", "sparkTestTopic")
            .save()
    }

}