package com.eklavya.scqla

import akka.actor.{ ActorRef, Actor }
import com.eklavya.scqla.Header._
import akka.io.Tcp._
import akka.util.ByteString
import Frame._
import scala.collection.mutable.Map

/**
 * Created by eklavya on 8/2/14.
 */
class Receiver extends Actor {

  implicit val sys = context.system

  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN
  
  val fulFillMap = Map.empty[Byte, ActorRef]
  
  // in case we are already connected and yet to be asked for confirmation
  var connected = false

  var sndr: ActorRef = null
  var scqla: ActorRef = null

  def receive = {
    case ShallWeStart =>
      scqla = sender

    case x: Connected =>
      sndr = sender
      context become {

        case ShallWeStart =>
          scqla = sender
          if (connected) scqla ! Start

        case FulFill(stream, actor) =>
          fulFillMap += (stream -> actor)

        case x: ByteString => {
          val b = x.iterator

          val stream = b.drop(2).getByte

          b.getByte match {

            case ERROR =>
              b.drop(4)
              val code = b.getInt
              val error = readString(b)
              fulFillMap(stream) ! Error(code, error)
              sndr ! FulFilled(stream)

            case READY =>
              println(s"Server is ready. from receiver $self")
              connected = true
              if (scqla != null) scqla ! Start
              sndr ! FulFilled(stream)

            case AUTHENTICATE =>
              println("Authentication required. Authenticating ...")
              sndr ! FulFilled(stream)
            //send from conf

            case SUPPORTED =>
              val len = b.getInt
              println(readMultiMap(b))
              sndr ! FulFilled(stream)

            case RESULT =>
              b.drop(4).getInt match {

                case VOID =>
                  fulFillMap(stream) ! Successful
                  println("Query was successful.")
                  sndr ! FulFilled(stream)

                case ROWS =>
                  fulFillMap(stream) ! ResultRows(parseRows(x.drop(12)))
                  sndr ! FulFilled(stream)

                case SET_KEYSPACE =>
                  fulFillMap(stream) ! SetKeyspace
                  println(s"keyspace is set to ${readString(b)}")
                  sndr ! FulFilled(stream)

                case PREPARED =>
                  val qid = readShortBytes(b).get
                  val cols = parseMetaData(b)
                  fulFillMap(stream) ! Prepared(qid, cols)
                  sndr ! FulFilled(stream)

                case SCHEMA_CHANGE =>
                  fulFillMap(stream) ! SchemaChange
                  println(s"${readString(b)} ${readString(b)} ${readString(b)}")
                  sndr ! FulFilled(stream)
              }

            case EVENT =>
              println(s"Strange we shouldn't get event notification on this connection.")
          }
        }
      }
  }

  val VERSION: Byte = 0x01
  val FLAGS: Byte = 0x00
  val consistency = ANY

}