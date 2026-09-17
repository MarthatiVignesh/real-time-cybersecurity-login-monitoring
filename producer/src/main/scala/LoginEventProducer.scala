import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import java.util.Properties
import java.time.Instant

object LoginEventProducer {

  def main(args: Array[String]): Unit = {

    val topic = "cybersecurity-logins"

    val properties = new Properties()

    properties.put(
      "bootstrap.servers",
      "localhost:9092"
    )

    properties.put(
      "key.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    properties.put(
      "value.serializer",
      "org.apache.kafka.common.serialization.StringSerializer"
    )

    val producer = new KafkaProducer[String, String](properties)

    var eventId = 1

    try {

      while (true) {

        val event = eventId match {

          case 1 =>
            createEvent(
              eventId,
              "user101",
              "Hyderabad",
              "LOGIN_FAILED"
            )

          case 2 =>
            createEvent(
              eventId,
              "user101",
              "Hyderabad",
              "LOGIN_FAILED"
            )

          case 3 =>
            createEvent(
              eventId,
              "user101",
              "Hyderabad",
              "LOGIN_FAILED"
            )

          case 4 =>
            createEvent(
              eventId,
              "user101",
              "USA",
              "LOGIN_SUCCESS"
            )

          case 5 =>
            createEvent(
              eventId,
              "user102",
              "Bangalore",
              "LOGIN_SUCCESS"
            )

          case 6 =>
            createEvent(
              eventId,
              "user103",
              "Chennai",
              "LOGIN_SUCCESS"
            )

          case 7 =>
            createEvent(
              eventId,
              "user104",
              "Hyderabad",
              "MFA_FAILED"
            )

          case 8 =>
            createEvent(
              eventId,
              "user104",
              "Hyderabad",
              "MFA_FAILED"
            )

          case 9 =>
            createEvent(
              eventId,
              "user105",
              "Delhi",
              "PASSWORD_RESET"
            )

          case 10 =>
            createEvent(
              eventId,
              "user106",
              "Mumbai",
              "ACCOUNT_LOCKED"
            )

          case _ =>
            createEvent(
              eventId,
              "user102",
              "Bangalore",
              "LOGIN_SUCCESS"
            )
        }

        val key =
          extractUserId(event)

        val record =
          new ProducerRecord[String, String](
            topic,
            key,
            event
          )

        producer.send(record)

        println(
          s"Sent: $event"
        )

        eventId += 1

        Thread.sleep(2000)
      }

    } finally {

      producer.close()

    }
  }

  def createEvent(
      id: Int,
      userId: String,
      location: String,
      eventType: String
  ): String = {

    s"""{
       |"event_id":"evt$id",
       |"user_id":"$userId",
       |"location":"$location",
       |"event_type":"$eventType",
       |"timestamp":"${Instant.now()}"
       |}""".stripMargin
  }

  def extractUserId(
      event: String
  ): String = {

    val pattern =
      """"user_id":"([^"]+)"""".r

    pattern
      .findFirstMatchIn(event)
      .map(_.group(1))
      .getOrElse("unknown")
  }
}
