package com.eklavya.scqla

import EventHandler._
import java.net.InetAddress

abstract class Event

abstract class NodeEvent extends Event
case object NewNodeEvent extends NodeEvent
case object RemovedNodeEvent extends NodeEvent
case object NodeUpEvent extends NodeEvent
case object NodeDownEvent extends NodeEvent

abstract class DBEvent extends Event
case object CreatedEvent extends DBEvent
case object UpdatedEvent extends DBEvent
case object DroppedEvent extends DBEvent

object Events {
  def registerNodeEvent(w: NodeEvent, f: InetAddress => Unit) = w match {
    case NewNodeEvent => register(NewNodeEvent, f)
    case RemovedNodeEvent => register(RemovedNodeEvent, f)
    case NodeUpEvent => register(NodeUpEvent, f)
    case NodeDownEvent => register(NodeDownEvent, f)
  }

  def deRegisterNodeEvent(w: NodeEvent, f: HandleNode) = w match {
    case NewNodeEvent => deRegister(NewNodeEvent, f)
    case RemovedNodeEvent => deRegister(RemovedNodeEvent, f)
    case NodeUpEvent => deRegister(NodeUpEvent, f)
    case NodeDownEvent => deRegister(NodeDownEvent, f)
  }

  def registerDBEvent(w: DBEvent, f: (String, String) => Unit) = w match {
    case CreatedEvent => register(CreatedEvent, f)
    case UpdatedEvent => register(UpdatedEvent, f)
    case DroppedEvent => register(DroppedEvent, f)
  }

  def deRegisterDBEvent(w: DBEvent, f: HandleDB) = w match {
    case CreatedEvent => deRegister(CreatedEvent, f)
    case UpdatedEvent => deRegister(UpdatedEvent, f)
    case DroppedEvent => deRegister(DroppedEvent, f)
  }
}