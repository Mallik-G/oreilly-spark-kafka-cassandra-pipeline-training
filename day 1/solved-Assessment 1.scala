
object Cells {
  sparkContext.stop()

  /* ... new cell ... */

  import twitter4j._

  /* ... new cell ... */

  val list = ul(20)
  list

  /* ... new cell ... */

  import akka.actor.Actor
  import akka.actor.ActorLogging

  /* ... new cell ... */

  object Utils {
    import java.util.Properties
    import org.apache.kafka.clients.producer.{KafkaProducer,ProducerConfig}
  
    def createProducer:KafkaProducer[String, String] = {
      val kafkaIP = "localhost"
      val props = new Properties()
      props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"${kafkaIP}:9092")
      props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
      val producer = new KafkaProducer[String, String](props)
      producer
    }
  }

  /* ... new cell ... */

  class TweetsCollector() extends Actor with ActorLogging {
    import org.apache.kafka.clients.producer.ProducerRecord
  
    val producer = Utils.createProducer
    
    override def preStart() = {}
    
    def toData(status: twitter4j.Status):String = {
      val name = status.getUser().getScreenName()
      val text = status.getText()
      s"${System.currentTimeMillis},${name},${text}" // name,text
      // @todo: probably shouldn't use , as a seperator since text message may contain them...
    }
   
    def receive = {
      case status: twitter4j.Status => 
        val d = toData(status)
        if (scala.util.Random.nextDouble < 0.001) list.append(d)
        
        producer.send(new ProducerRecord[String, String]("twitter", null, d))
    } 
  }

  /* ... new cell ... */

  implicit val system:akka.actor.ActorSystem = akka.actor.ActorSystem()

  /* ... new cell ... */

  import system.dispatcher
  import akka.actor.Props
  val streamActor:akka.actor.ActorRef = system.actorOf(Props(new TweetsCollector()))

  /* ... new cell ... */

  val twitterStream = new TwitterStreamFactory().getInstance()
  val listener = new StatusListener() {
      def onStatus(status: Status) {
        streamActor ! status // send to actor, check http://doc.akka.io/api/akka/2.4.2/#akka.actor.ActorRef
      }
      def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice) {}
      def onTrackLimitationNotice(numberOfLimitedStatuses: Int) {}
      def onException(ex:java.lang.Exception) {
          ex.printStackTrace()
      }
      def onScrubGeo(lat: Long, long: Long){}
    def onStallWarning(s: twitter4j.StallWarning) {}
  }
  twitterStream.addListener(listener)

  /* ... new cell ... */

  import scala.language.postfixOps
  var alreadyApplied = false
  val form = Form[List[String]](
    List("<consumer key>", "<consumer secret>", "<token>", "<token secret>"), 
    "Set Twitter App Auth. and Start Streaming", 
    paramsToData = m => m.toList.sortBy(_._1).map(_._2), 
    dataToParams = l => List("consumerKey", "consumerSecret", "token", "tokenSecret") zip l toMap, 
    true, false
  ) { l =>
    if (!alreadyApplied) {
      alreadyApplied = true
  
      val List(consumerKey, consumerSecret, token, tokenSecret) = l
      twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
      twitterStream.setOAuthAccessToken(new auth.AccessToken(token, tokenSecret));
      
      twitterStream.sample()
    }
  }
  form

  /* ... new cell ... */

  //shutdown stream
  twitterStream.shutdown

  /* ... new cell ... */
}
                  