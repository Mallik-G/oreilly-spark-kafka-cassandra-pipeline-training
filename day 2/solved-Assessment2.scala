
object Cells {
  import com.datastax.spark.connector.cql.CassandraConnector
  import com.datastax.spark.connector._

  /* ... new cell ... */

  val cc = CassandraConnector(sparkContext.getConf)

  /* ... new cell ... */

  cc.withSessionDo { session => 
                    session.execute(s"""
                      CREATE KEYSPACE IF NOT EXISTS pipeline 
                      WITH REPLICATION = { 'class':'SimpleStrategy', 'replication_factor':1}
                    """.stripMargin
                    )
                   }

  /* ... new cell ... */

  cc.withSessionDo { session => 
                    session.execute(s"""
                      DROP TABLE IF EXISTS pipeline.avg_price
                    """
                    )}

  /* ... new cell ... */

  cc.withSessionDo { session => 
                    session.execute(s"""
                    CREATE TABLE pipeline.avg_price (
                      symbol text,
                      ts timestamp,
                      price double,
                      PRIMARY KEY (symbol, ts)
                    )
                    """
                    )}

  /* ... new cell ... */

  object model extends Serializable {
    object Quote extends Serializable {
      val pat = """(\d+),[^,]*,\"([^"]+)\",[^,]*,[^,]*,[^,]*,[^,]*,[^,]*,([^,]*),(\d+),.*""".r
      def parse(s: String): Option[Quote] = {
        try {
          pat.findFirstMatchIn(s).map(x => Quote(x.subgroups(1), 1000*(x.subgroups(0).toLong/1000),  x.subgroups(2).toDouble, x.subgroups(3).toLong))
        } catch {
          case _ : Throwable => None
        }
      }
    } 
    
    case class Quote(symbol: String, ts: Long, price: Double, volume: Long)
  }

  /* ... new cell ... */

  

  /* ... new cell ... */

  @transient val sqlContext = new org.apache.spark.sql.SQLContext(sparkContext)
  import sqlContext.implicits._
  import org.apache.spark.sql.functions._

  /* ... new cell ... */

  import org.apache.spark.streaming.Seconds
  import org.apache.spark.streaming.StreamingContext
  
  StreamingContext.getActive.foreach(_.stop(false))
  @transient val ssc = new StreamingContext(sc, Seconds(20))
  val brokers = "localhost:9092"
  val topics = Set("quotes")

  /* ... new cell ... */

  //val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
  
  // OR START AT SMALLEST OFFSET:
  val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, 
                                        "auto.offset.reset"    -> "smallest")

  /* ... new cell ... */

  import org.apache.spark.streaming.kafka.KafkaUtils
  import org.apache.spark.streaming.Time
  import kafka.serializer.StringDecoder
  
  // connect (direct) to kafka
  @transient val ratingsStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topics)

  /* ... new cell ... */

  // transform the CSV Strings into Ratings
  @transient val msgs = ratingsStream.transform { (message: RDD[(String, String)], batchTime: Time) => {
      // convert each RDD from the batch into a DataFrame
      message.map(s => model.Quote.parse(s._2)).collect{case Some(q) => q}
    }
  }

  /* ... new cell ... */

  @transient val aggs = msgs.transform { rdd => 
                                        val ts =  rdd.map(quote => quote.ts).max    /// maximum of timestamp in the rdd
                                        rdd.map{ quote => 
                                          val tuple = ((quote.symbol, ts), (quote.price, 1))          /// ((String, Long), (Double, Long)) structure
                                          tuple
                                         }
                                        }
                            .reduceByKey{ (x,y) => (x._1 + y._1, x._2 + y._2) }   // returns (Double, Long)
                            .mapValues(pair => pair._1 / pair._2 )             // (Double, Long) => (Double)
                            .map{ case ((s,t),v) => (s,t,v)}
  
    

  /* ... new cell ... */

  @transient val comp = aggs.foreachRDD {rdd => 
                     rdd.toDF("symbol", "ts", "price").write.format("org.apache.spark.sql.cassandra")
                        .mode(org.apache.spark.sql.SaveMode.Append)
                        .options(Map("keyspace" -> "pipeline", "table" -> "avg_price"))
                           .save()
                   }

  /* ... new cell ... */

  ssc.start()

  /* ... new cell ... */
}
                  