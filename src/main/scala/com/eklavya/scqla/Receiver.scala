package com.eklavya.scqla

import akka.actor.{ActorRef, Actor}
import com.eklavya.scqla.Header._
import akka.io.Tcp._
import akka.util.{ByteString, ByteStringBuilder}
import Scqla._
import Frame._
import scala.collection.mutable.Map

/**
 * Created by eklavya on 8/2/14.
 */
class Receiver extends Actor {

  implicit val sys = context.system

  val fullFillMap = Map.empty[Byte, ActorRef]

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

            //READY
            case 0x02 =>
              println("Server is ready.")
              scqla ! Start
              sndr ! FullFilled(stream)

            //AUTHENTICATE
            case 0x03 =>
              println("Authentication required. Authenticating ...")
              //send from conf

            //SUPPORTED
            case 0x06 =>
              val len = b.getInt
              println(readMultiMap(b))

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
                  println(s"${readString(b)} ${readString(b)} ${readString(b)}")

              }

            //EVENT
            case 0x0C =>
          }
        }
      }
  }

  val VERSION: Byte = 0x01
  val FLAGS: Byte = 0x00
  val consistency = ANY
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

}