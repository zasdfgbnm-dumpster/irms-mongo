import org.mongodb.scala.{Observable=>MongoObservable,_}
import org.mongodb.scala.bson.collection.immutable
import org.mongodb.scala.model._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import scala.io._
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.math._
import rx.lang.scala._
import java.util.concurrent.Semaphore
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import rxScala.Implicits._


package irms {

	object ImportXYZ {

		val pt = "H He Li Be B C N O F Ne Na Mg Al Si P S Cl Ar K Ca Sc Ti V Cr Mn Fe Co Ni Cu Zn Ga Ge As Se Br Kr Rb Sr Y Zr Nb Mo Tc Ru Rh Pd Ag Cd In Sn Sb Te I Xe Cs Ba La Ce Pr Nd Pm Sm Eu Gd Tb Dy Ho Er Tm Yb Lu Hf Ta W Re Os Ir Pt Au Hg Tl Pb Bi Po At Rn Fr Ra Ac Th Pa U Np Pu Am Cm Bk Cf Es Fm Md No Lr Rf Db Sg Bh Hs Mt Ds Rg Cn Nh Fl Mc Lv Ts Og"
		val elements = pt.split(" ")
		val tol = 1e-5

		val max_queries:Int = 40
		val batch_size:Int = 10000 // 1 batch contain 1000 elements
		val max_batches_per_file:Int = 100 // 1 file contain 100 batches

		// db commons
		val mongoClient: MongoClient = MongoClient()
		val database: MongoDatabase = mongoClient.getDatabase("irms")
		val collection: MongoCollection[Document] = database.getCollection("universe")

		// convert strings like "23;6,2.7715,0.006,0.351;6,1.9331,-1.1897,0.1199;6,0.7004,-0.7605,-0.0479;6,-0.5601,-1.5308,-0.3146;8,-1.3413,-1.4834,0.8926;6,-1.7422,-0.2751,1.4754;6,-1.5296,0.9971,1.119;6,-0.7287,1.3521,-0.114;6,-1.3883,0.6465,-1.2833;8,-0.5908,0.0376,-2.2552;6,-1.3106,-0.7519,-1.3638;6,0.6091,0.6917,0.0513;6,1.8225,1.1218,0.2834;1,3.3032,-0.0689,1.3188;1,3.5028,0.1291,-0.5007;1,2.2614,-2.2373,0.0912;1,-0.3807,-2.557,-0.6179;1,-2.3406,-0.3713,2.3924;1,-1.9293,1.7857,1.7105;1,-0.6423,2.4286,-0.2426;1,-2.3145,1.1755,-1.6411;1,-2.1391,-1.3407,-1.8266;1,2.034,2.1949,0.4021;"
		// to document like:
		// 	{
		// 		3d_struct: [
		// 			{ type="C",xyz=[2.7715,0.006,0.351] },
		// 		]
		// 	}
		def str2doc(str:String):(Boolean,List[immutable.Document]) = {
			val a = str.split(";").drop(1).map(_.split(","))
			def a2doc(a:Array[String]):(Boolean,immutable.Document) = {
				val xyz = a.drop(1).map(_.toDouble).toList
				val is_origin = xyz.map(abs(_)<tol).reduce(_&&_)
				(is_origin,Document("type"->elements(a(0).toInt-1),"xyz"->xyz))
			}
			val docs = a.map(a2doc).toList
			val bad = docs.length>1 && docs.map(_._1).reduce(_&&_)
			(bad,docs.map(_._2))
		}

		def str2update(str:String) = {
			val a = str.split("\\s+")
			val smiles = a(0)
			val conv = str2doc(a(1))
			( conv._1, UpdateOneModel(Document("_id" -> smiles), set("3d_struct", conv._2)) )
		}

		def lines2updates(l:Seq[String]):List[UpdateOneModel[Nothing]] = {
			l.par.map(_.trim).par.filter(_.length>0).par.map(str2update).filter(!_._1).map(_._2).toList
		}

		def updates2bulkwrite(w:List[UpdateOneModel[Nothing]]) = {
			collection.bulkWrite(w, BulkWriteOptions().ordered(false))
		}

		def main(args: Array[String]): Unit = {
			// construct observables
			val readyToExit = new Semaphore(0)
			val allowedFiles = new Semaphore(max_batches_per_file)
			val allowedBatches = new Semaphore(max_queries)
			//45 to 9658
			val streamFiles = Observable.from( args(0).toInt to args(1).toInt ).flatMap(
				j => Observable.from(Future {
					allowedFiles.acquire(max_batches_per_file)
					println("file: "+j)
					"/mnt/data/gaoxiang/raw/xyz/univ-%05d".format(j)
				}
			));
			val streamBatches = streamFiles.flatMap( (fn:String) => {
				val batches = Source.fromFile(fn).getLines.grouped(batch_size).toList
				val obatches = Observable.from(batches)
				if (max_batches_per_file-batches.length>0)
					allowedFiles.release(max_batches_per_file-batches.length)
				obatches.flatMap(
					j => Observable.from(Future {
						allowedBatches.acquire()
						allowedFiles.release()
						j
					})
				)
			})
			val streamUpdates = streamBatches.map(lines2updates)
			val streamBulkWrite = streamUpdates.flatMap(updates2bulkwrite)
			streamBulkWrite.subscribe(
				// onNext
				j => allowedBatches.release() ,
				// onError
				(j:Throwable) => {
					println(j.getMessage)
					j.printStackTrace
				},
				// onCompleted
				() => readyToExit.release()
			)


			// clean up
			readyToExit.acquire()
			mongoClient.close()
		}
	}

}
