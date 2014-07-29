package com.eklavya.scqla

import java.net.InetAddress

import com.eklavya.scqla.Scqla.ec

import scala.collection.mutable.Set
import scala.concurrent.Future

object EventHandler {
  
  type HandleNode = Function[InetAddress, Unit]
  
  type HandleDB = Function2[String, String, Unit]
  
  val newNodeEvents = Set.empty[HandleNode]
  
  val removedNodeEvents = Set.empty[HandleNode]
  
  val upNodeEvents = Set.empty[HandleNode]
  
  val downNodeEvents = Set.empty[HandleNode]
  
  val createdEvents = Set.empty[HandleDB]
  
  val updatedEvents = Set.empty[HandleDB]
  
  val droppedEvents = Set.empty[HandleDB]
  
  def register(e: NodeEvent, f: Function[InetAddress, Unit]): HandleNode = e match {
    case NewNodeEvent => newNodeEvents.synchronized {
      newNodeEvents += f
      f
    }
    case RemovedNodeEvent => removedNodeEvents.synchronized {
      removedNodeEvents += f
      f
    }
    case NodeUpEvent => upNodeEvents.synchronized {
      upNodeEvents += f
      f
    }
    case NodeDownEvent => downNodeEvents.synchronized {
      downNodeEvents += f
      f
    }
  }
  
  def deRegister(e: NodeEvent, f: HandleNode) = e match {
    case NewNodeEvent => newNodeEvents.synchronized {
      newNodeEvents -= f
    }
    case RemovedNodeEvent => removedNodeEvents.synchronized {
      removedNodeEvents -= f
    }
    case NodeUpEvent => upNodeEvents.synchronized {
      upNodeEvents -= f
    }
    case NodeDownEvent => downNodeEvents.synchronized {
      downNodeEvents -= f
    }
  }
  
  def register(e: DBEvent, f: Function2[String, String, Unit]): HandleDB = e match {
    case CreatedEvent => createdEvents.synchronized {
      createdEvents += f
      f
    }
    case UpdatedEvent => updatedEvents.synchronized {
      updatedEvents += f
      f
    }
    case DroppedEvent => droppedEvents.synchronized {
      droppedEvents += f
      f
    }
  }
  
  def deRegister(e: DBEvent, f: HandleDB) = e match {
    case CreatedEvent => createdEvents.synchronized {
      createdEvents -= f
    }
    case UpdatedEvent => updatedEvents.synchronized {
      updatedEvents -= f
    }
    case DroppedEvent => droppedEvents.synchronized {
      droppedEvents -= f
    }
  }
  
  def nodeEvent(e: NodeEvent, addr: InetAddress): Unit = e match {
    case NewNodeEvent => newNodeEvents.foreach(f => Future{f(addr)})
    case RemovedNodeEvent => removedNodeEvents.foreach(f => Future(f(addr)))
    case NodeUpEvent => upNodeEvents.foreach(f => Future(f(addr)))
    case NodeDownEvent => downNodeEvents.foreach(f => Future(f(addr)))
  }
  
  def dbEvent(e: DBEvent, keySpace: String, table: String): Unit = e match {
    case CreatedEvent => createdEvents.foreach(f => Future(f(keySpace, table)))
    case UpdatedEvent => updatedEvents.foreach(f => Future(f(keySpace, table)))
    case DroppedEvent => droppedEvents.foreach(f => Future(f(keySpace, table)))
  }
}