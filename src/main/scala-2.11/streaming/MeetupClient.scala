package streaming

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import com.github.andyglow.websocket._
import common.Data._
import common.FileSystem

/**
  * Created by Dennis Hunziker on 18/11/16.
  */
object MeetupClient extends App {

  // Data directory cleanup:
  // rm /tmp/rsvp/* && watch -n 5 "ls /tmp/rsvp | wc -l"

  val counter = new AtomicInteger()

  val client = WebsocketClient[String]("ws://stream.meetup.com/2/rsvps") {
    case json => {
      FileSystem.writeStringToFile(json, s"${RsvpDir}/rsvp-${UUID.randomUUID()}.json")
      if (counter.incrementAndGet() % 100 == 0) println(s"Written ${counter.get()} files")
    }
  }

  val ws = client.open()

}
