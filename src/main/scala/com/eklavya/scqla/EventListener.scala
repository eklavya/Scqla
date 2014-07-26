package com.eklavya.scqla

import akka.actor.Actor
import java.net.InetSocketAddress
import akka.io.Tcp.ConnectionClosed
import akka.io.Tcp.Register
import akka.io.Tcp.Received
import akka.io.Tcp.CommandFailed
import akka.io.IO
import akka.actor.ActorRef
import akka.io.Tcp.Connected
// import akka.actor.IO.Close
import akka.io.Tcp.Write
import akka.io.Tcp.Connect
import akka.io.Tcp
import Frame._

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
    //
    //    case c @ Credentials =>
    //
    //    case o @ Options =>
    //
    //    case r @ Register =>

    case ShallWeStart =>
          scqla = sender
          if (registered) scqla ! Start

    case CommandFailed(w: Write) =>

    case Received(x) =>
      val b = x.iterator

      val stream = b.drop(2).getByte

      b.getByte match {

        //ERROR
        case 0x00 =>
          b.drop(4)
          println(s"Error Code: ${b.getInt}")
          println(readString(b))
          context stop self

        //READY
        case 0x02 =>
          if (!registered) {
            println(s"Server is ready from receiver $self, registering for events.")
            registered = true
            if (scqla != null) scqla ! Start
            connHandle ! Write(registerFrame(writeList(List("TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE")), 1))
          }

        //AUTHENTICATE
        case 0x03 =>
          println("Authentication required. Authenticating ...")
        //send from conf

        //EVENT
        case 0x0C =>
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