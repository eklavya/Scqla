package com.eklavya.scqla

import java.net.InetSocketAddress

import akka.actor.{Actor, ActorRef}
import akka.io.{IO, Tcp}
import akka.io.Tcp.{CommandFailed, Connect, Connected, ConnectionClosed, Received, Register, Write}
import com.eklavya.scqla.Frame._
import com.eklavya.scqla.Header._

class EventListener(host: String, port: Int) extends Actor {

  val remote = new InetSocketAddress(host, port)

  var connHandle: ActorRef = _

  var scqla: ActorRef = _

  var registered = false

  implicit val sys = context.system

  IO(Tcp) ! Connect(remote)

  def receive: Receive = {

    case CommandFailed(_: Connect) =>
      context stop self

    case c @ Connected(remote, local) =>
      connHandle = sender
      connHandle ! Register(self)
      connHandle ! Write(startupFrame(0))
      context become connected

    case ShallWeStart =>
      scqla = sender
  }

  def connected: Receive = {

    case ShallWeStart =>
          scqla = sender
          if (registered) scqla ! Start

    case CommandFailed(w: Write) =>

    case Received(x) =>
      val b = x.iterator

      val stream = b.drop(2).getByte

      b.getByte match {

        case ERROR =>
          b.drop(4)
          println(s"Error Code: ${b.getInt}")
          println(readString(b))
          context stop self

        case READY =>
          if (!registered) {
            println(s"Server is ready from receiver $self, registering for events.")
            registered = true
            if (scqla != null) scqla ! Start
            connHandle ! Write(registerFrame(writeList(List("TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE")), 1))
          }

        case AUTHENTICATE =>
          println("Authentication required. Authenticating ...")
        //send from conf

        case EVENT =>
          b.drop(4)
          readString(b) match {

            case "TOPOLOGY_CHANGE" =>
              val eventType = readString(b)
              eventType match {
                case "NEW_NODE" => EventHandler.nodeEvent(NewNodeEvent, readInet(b))
                case "REMOVED_NODE" => EventHandler.nodeEvent(RemovedNodeEvent, readInet(b))
              }

            case "STATUS_CHANGE" =>
              val eventType = readString(b)
              eventType match {
                case "UP" => EventHandler.nodeEvent(NodeUpEvent, readInet(b))
                case "DOWN" => EventHandler.nodeEvent(NodeDownEvent, readInet(b))
              }

            case "SCHEMA_CHANGE" =>
              val eventType = readString(b)
              eventType match {
                case "CREATED" => EventHandler.dbEvent(CreatedEvent, readString(b), readString(b))
                case "UPDATED" => EventHandler.dbEvent(UpdatedEvent, readString(b), readString(b))
                case "DROPPED" => EventHandler.dbEvent(DroppedEvent, readString(b), readString(b))
              }
          }
      }

    case _: ConnectionClosed => context stop self
  }

}