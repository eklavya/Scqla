package com.eklavya.scqla

import akka.actor.{ ActorRef, Actor }
import com.eklavya.scqla.Header._
import akka.io.Tcp._
import akka.util.{ ByteString, ByteStringBuilder }
import Scqla._
import Frame._
import scala.collection.mutable.Map

/**
 * Created by eklavya on 8/2/14.
 */
class Receiver extends Actor {

  implicit val sys = context.system

  val fullFillMap = Map.empty[Byte, ActorRef]
  
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

        case FullFill(stream, actor) =>
          fullFillMap += (stream -> actor)

        case x: ByteString => {
          val b = x.iterator

          val stream = b.drop(2).getByte

          b.getByte match {

            //ERROR
            case 0x00 =>
              b.drop(4)
              println(s"Error Code: ${b.getInt}")
              println(readString(b))
              sndr ! FullFilled(stream)

            //READY
            case 0x02 =>
              println(s"Server is ready. from receiver $self")
              connected = true
              if (scqla != null) scqla ! Start
              sndr ! FullFilled(stream)

            //AUTHENTICATE
            case 0x03 =>
              println("Authentication required. Authenticating ...")
              sndr ! FullFilled(stream)
            //send from conf

            //SUPPORTED
            case 0x06 =>
              val len = b.getInt
              println(readMultiMap(b))
              sndr ! FullFilled(stream)

            //RESULT
            case 0x08 =>
              b.drop(4).getInt match {

                case VOID =>
                  fullFillMap(stream) ! Successful
                  println("Query was successful.")
                  sndr ! FullFilled(stream)

                case ROWS =>
                  fullFillMap(stream) ! ResultRows(parseRows(x.drop(12)))
                  sndr ! FullFilled(stream)

                case SET_KEYSPACE =>
                  fullFillMap(stream) ! SetKeyspace
                  println(s"keyspace is set to ${readString(b)}")
                  sndr ! FullFilled(stream)

                case PREPARED =>
                  val qid = readShortBytes(b).get
                  val cols = parseMetaData(b)
                  fullFillMap(stream) ! Prepared(qid, cols)
                  sndr ! FullFilled(stream)

                case SCHEMA_CHANGE =>
                  fullFillMap(stream) ! SchemaChange
                  println(s"${readString(b)} ${readString(b)} ${readString(b)}")
                  sndr ! FullFilled(stream)
              }

            //EVENT
            case 0x0C =>
              sndr ! FullFilled(stream)
          }
        }
      }
  }

  val VERSION: Byte = 0x01
  val FLAGS: Byte = 0x00
  val consistency = ANY
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

}