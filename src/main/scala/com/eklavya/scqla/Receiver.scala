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

            //ERROR
            case 0x00 =>
              b.drop(4)
              println(s"Error Code: ${b.getInt}")
              println(readString(b))
              sndr ! FulFilled(stream)

            //READY
            case 0x02 =>
              println(s"Server is ready. from receiver $self")
              connected = true
              if (scqla != null) scqla ! Start
              sndr ! FulFilled(stream)

            //AUTHENTICATE
            case 0x03 =>
              println("Authentication required. Authenticating ...")
              sndr ! FulFilled(stream)
            //send from conf

            //SUPPORTED
            case 0x06 =>
              val len = b.getInt
              println(readMultiMap(b))
              sndr ! FulFilled(stream)

            //RESULT
            case 0x08 =>
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

            //EVENT
            case 0x0C =>
              sndr ! FulFilled(stream)
          }
        }
      }
  }

  val VERSION: Byte = 0x01
  val FLAGS: Byte = 0x00
  val consistency = ANY
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

}