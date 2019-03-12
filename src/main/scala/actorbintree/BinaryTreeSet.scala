/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package actorbintree

import actorbintree.BinaryTreeNode.{CopyFinished, CopyTo}
import actorbintree.BinaryTreeSet._
import akka.actor._

import scala.collection.immutable.Queue

object BinaryTreeSet {

  trait Operation {
    def requester: ActorRef
    def id: Int
    def elem: Int
  }

  trait OperationReply {
    def id: Int
  }

  /** Request with identifier `id` to insert an element `elem` into the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Insert(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to check whether an element `elem` is present
    * in the tree. The actor at reference `requester` should be notified when
    * this operation is completed.
    */
  case class Contains(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request with identifier `id` to remove the element `elem` from the tree.
    * The actor at reference `requester` should be notified when this operation
    * is completed.
    */
  case class Remove(requester: ActorRef, id: Int, elem: Int) extends Operation

  /** Request to perform garbage collection*/
  case object GC

  /** Holds the answer to the Contains request with identifier `id`.
    * `result` is true if and only if the element is present in the tree.
    */
  case class ContainsResult(id: Int, result: Boolean) extends OperationReply
  
  /** Message to signal successful completion of an insert or remove operation. */
  case class OperationFinished(id: Int) extends OperationReply

}


class BinaryTreeSet extends Actor {
  import BinaryTreeSet._

  def createRoot: ActorRef = context.actorOf(BinaryTreeNode.props(0, initiallyRemoved = true))

  var root = createRoot

  // optional
  var pendingQueue = Queue.empty[Operation]

  // optional
  def receive = normal

  // optional
  /** Accepts `Operation` and `GC` messages. */
  val normal: Receive = {
    case operation: Operation => root ! operation
    case GC => {
      val newRoot = createRoot
      root ! CopyTo(newRoot)
      context become(garbageCollecting(newRoot))
    }
  }

  // optional
  /** Handles messages while garbage collection is performed.
    * `newRoot` is the root of the new binary tree where we want to copy
    * all non-removed elements into.
    */
  def garbageCollecting(newRoot: ActorRef): Receive = {
    case operation: Operation => pendingQueue = pendingQueue.enqueue(operation)
    case CopyFinished => {
      root ! PoisonPill
      root = newRoot
      pendingQueue.foreach{ i =>
        root ! i
      }
      pendingQueue = Queue.empty[Operation]
      context.become(normal)
    }
  }

}

object BinaryTreeNode {
  trait Position

  case object Left extends Position
  case object Right extends Position

  case class CopyTo(treeNode: ActorRef)
  case object CopyFinished

  def props(elem: Int, initiallyRemoved: Boolean) = Props(classOf[BinaryTreeNode],  elem, initiallyRemoved)
}

class BinaryTreeNode(val elem: Int, initiallyRemoved: Boolean) extends Actor {
  import BinaryTreeNode._

  var subtrees = Map[Position, ActorRef]()
  var removed = initiallyRemoved

  // optional
  def receive = normal

  // optional
  /** Handles `Operation` messages and `CopyTo` requests. */
  val normal: Receive = {
    case Insert(req, id, elem) => {
      val binaryTreeNode = context.actorOf(Props(new BinaryTreeNode(elem, false)))
      elem match {
        case e if e < this.elem => {
          subtrees.contains(Left) match {
            case true => {
              val left = subtrees(Left)
              left ! Insert(req, id, e)
            }
            case false =>
              subtrees = subtrees + (Left -> binaryTreeNode)
              req ! OperationFinished(id)
          }
        }
        case e if e > this.elem => {
          subtrees.contains(Right) match {
            case true => {
              val right = subtrees(Right)
              right ! Insert(req, id, e)
            }
            case false => {
              subtrees = subtrees + (Right -> binaryTreeNode)
              req ! OperationFinished(id)
            }
          }
        }
        case e if e == this.elem => req ! OperationFinished(id)
      }
    }
    case Contains(req, id, elem) => {
      elem match {
        case e if e == this.elem && !this.removed => req ! ContainsResult(id, true)
        case e if e < this.elem && subtrees.contains(Left) => {
          val left = subtrees(Left)
          left ! Contains(req, id, e)
        }
        case e if e > this.elem && subtrees.contains(Right) => {
          val right = subtrees(Right)
          right ! Contains(req, id, e)
        }
        case _ => req ! ContainsResult(id, false)
      }
    }
    case Remove(req, id, elem) => {
      elem match {
        case e if e == this.elem => {
          this.removed = true
          req ! OperationFinished(id)
        }
        case e if e < this.elem && subtrees.contains(Left) => {
          val left = subtrees(Left)
          left ! Remove(req, id, e)
        }
        case e if e > this.elem && subtrees.contains(Right)=> {
          val right = subtrees(Right)
          right ! Remove(req, id, e)
        }
        case _ => req ! OperationFinished(id)
      }
    }
    case CopyTo(treeNode) => {
      (this.removed, subtrees.isEmpty) match {
        case (true, true) => sender ! CopyFinished
        case (false, _) => {
          treeNode ! Insert(self, 0, elem)
          subtrees.values.foreach(_ ! CopyTo(treeNode))
        }
        case (_, _) => context.become(copying(subtrees.values.toSet, insertConfirmed = removed))
      }
    }
  }

  // optional
  /** `expected` is the set of ActorRefs whose replies we are waiting for,
    * `insertConfirmed` tracks whether the copy of this node to the new tree has been confirmed.
    */
  def copying(expected: Set[ActorRef], insertConfirmed: Boolean): Receive = {
    case OperationFinished(_) => {
      expected.isEmpty match {
        case true => {
          sender ! CopyFinished
          context.become(normal)
        }
        case false => context.become(copying(expected, true))
      }
    }
    case CopyFinished =>
      val newExpected = expected - sender
      (newExpected.isEmpty, insertConfirmed) match {
        case (true, true) => {
          sender ! CopyFinished
          context.become(normal)
        }
        case (_, _) => context.become(copying(newExpected, insertConfirmed))
      }
  }
}
