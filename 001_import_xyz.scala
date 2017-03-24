import org.mongodb.scala._
import org.mongodb.scala.model._
import org.mongodb.scala.model.Filters._
import org.mongodb.scala.model.Updates._
import scala.io._
import java.util.concurrent.TimeUnit
import scala.concurrent.Await
import scala.concurrent.duration.Duration

package irms {

	object ImportXYZ {

		val pt = "H He Li Be B C N O F Ne Na Mg Al Si P S Cl Ar K Ca Sc Ti V Cr Mn Fe Co Ni Cu Zn Ga Ge As Se Br Kr Rb Sr Y Zr Nb Mo Tc Ru Rh Pd Ag Cd In Sn Sb Te I Xe Cs Ba La Ce Pr Nd Pm Sm Eu Gd Tb Dy Ho Er Tm Yb Lu Hf Ta W Re Os Ir Pt Au Hg Tl Pb Bi Po At Rn Fr Ra Ac Th Pa U Np Pu Am Cm Bk Cf Es Fm Md No Lr Rf Db Sg Bh Hs Mt Ds Rg Cn Nh Fl Mc Lv Ts Og"
		val elements = pt.split(" ")

		// convert strings like "23;6,2.7715,0.006,0.351;6,1.9331,-1.1897,0.1199;6,0.7004,-0.7605,-0.0479;6,-0.5601,-1.5308,-0.3146;8,-1.3413,-1.4834,0.8926;6,-1.7422,-0.2751,1.4754;6,-1.5296,0.9971,1.119;6,-0.7287,1.3521,-0.114;6,-1.3883,0.6465,-1.2833;8,-0.5908,0.0376,-2.2552;6,-1.3106,-0.7519,-1.3638;6,0.6091,0.6917,0.0513;6,1.8225,1.1218,0.2834;1,3.3032,-0.0689,1.3188;1,3.5028,0.1291,-0.5007;1,2.2614,-2.2373,0.0912;1,-0.3807,-2.557,-0.6179;1,-2.3406,-0.3713,2.3924;1,-1.9293,1.7857,1.7105;1,-0.6423,2.4286,-0.2426;1,-2.3145,1.1755,-1.6411;1,-2.1391,-1.3407,-1.8266;1,2.034,2.1949,0.4021;"
		// to document like:
		// 	{
		// 		3d_struct: [
		// 			{ type="C",xyz=[2.7715,0.006,0.351] },
		// 		]
		// 	}
		def str2doc(str:String):List[Document] = {
			val a = str.split(";").drop(1).map(_.split(","))
			def a2doc(a:Array[String]):Document =
				Document("type"->elements(a(0).toInt-1),"xyz"->a.drop(1).map(_.toDouble).toList)
			a.map(a2doc).toList
		}

		def str2update(str:String) = {
			val a = str.split("\\s+")
			val smiles = a(0)
			UpdateOneModel(Document("_id" -> smiles), set("3d_struct", str2doc(a(1))))
		}

		def wait_results[C](observable:Observable[C]): Seq[C] = Await.result(observable.toFuture(), Duration.Inf)

		def main(args: Array[String]): Unit = {
			val fn = if(args.length==0) "/home/gaoxiang/irms/irms-mongo/test.txt" else args(0)
			val fileLines = Source.fromFile(fn).getLines.toList
			val writes: List[WriteModel[_ <: Document]] = fileLines.par.map(_.trim).par.filter(_.length>0).par.map(str2update).toList
			val mongoClient: MongoClient = MongoClient()
			val database: MongoDatabase = mongoClient.getDatabase("test")
			val collection: MongoCollection[Document] = database.getCollection("test")
			val op = collection.bulkWrite(writes, BulkWriteOptions().ordered(false))
			wait_results(op)
			mongoClient.close()
		}
	}

}
