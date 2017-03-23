import org.mongodb.scala._
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration

package irms {

	object ImportXYZ {

		def wait_results[C](observable:Observable[C]): Seq[C] = Await.result(observable.toFuture(), Duration.Inf)

		def main(args: Array[String]): Unit = {
			val docseq = Seq(Document("test"->"test"))
			val mongoClient: MongoClient = MongoClient()
			val database: MongoDatabase = mongoClient.getDatabase("test")
			val collection: MongoCollection[Document] = database.getCollection("test")
			println("begin inserting " + docseq.length + "data into db, wait patiently...")
			wait_results(collection.insertMany(docseq))
			println("done inserting " + docseq.length + "data into db.")
			mongoClient.close()
		}
	}

}
