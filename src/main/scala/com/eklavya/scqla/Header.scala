package com.eklavya.scqla

case class Header(val version: Byte, val flags: Byte, val stream: Byte, val opCode: Byte, val length: Int)

object Header {

  def fromBytes(b: Array[Byte]) = {
    val decoded  = b
    val length = 0
    Header(b(0), b(1), b(2), b(3), length)
  }

  val header = Array[Byte](8)

  // Messages
  val ERROR: Byte        = 0x00
  val STARTUP: Byte      = 0x01
  val READY: Byte        = 0x02
  val AUTHENTICATE: Byte = 0x03
  val CREDENTIALS: Byte  = 0x04
  val OPTIONS: Byte      = 0x05
  val SUPPORTED: Byte    = 0x06
  val QUERY: Byte        = 0x07
  val RESULT: Byte       = 0x08
  val PREPARE: Byte      = 0x09
  val EXECUTE: Byte      = 0x0A
  val REGISTER: Byte     = 0x0B
  val EVENT: Byte        = 0x0C

  // Data Types
  val CUSTOM: Short           = 0x0000
  val ASCII: Short            = 0x0001
  val BIGINT: Short           = 0x0002
  val BLOB: Short             = 0x0003
  val BOOLEAN: Short          = 0x0004
  val COUNTER: Short          = 0x0005
  val DECIMAL: Short          = 0x0006
  val DOUBLE: Short           = 0x0007
  val FLOAT: Short            = 0x0008
  val INT: Short              = 0x0009
  val TEXT: Short             = 0x000A
  val TIMESTAMP: Short        = 0x000B
  val UUID: Short             = 0x000C
  val VARCHAR: Short          = 0x000D
  val VARINT: Short           = 0x000E
  val TIMEUUID: Short         = 0x000F
  val INET: Short             = 0x0010
  val LIST: Short             = 0x0020
  val MAP: Short              = 0x0021
  val SET: Short              = 0x0022



  /***************** Error Codes ******************/

  /*
  something unexpected happened. This indicates a server-side bug.
  */
  val SERVER_ERROR     = 0x0000

  /* some client message triggered a protocol violation
   * (for instance a QUERY message is sent before a STARTUP one has been sent)
   */
  val PROTOCOL_ERROR   = 0x000A

  /*
  * CREDENTIALS request failed because Cassandra
  * did not accept the provided credentials.
  */
  val BAD_CREDENTIALS  = 0x0100

  /*
   Unavailable exception. The rest of the ERROR message body will be
                <cl><required><alive>
              where:
                <cl> is the [consistency] level of the query having triggered
                     the exception.
                <required> is an [int] representing the number of node that
                           should be alive to respect <cl>
                <alive> is an [int] representing the number of replica that
                        were known to be alive when the request has been
                        processed (since an unavailable exception has been
                        triggered, there will be <alive> < <required>)

  */
  val UNAVAILABLE      = 0x1000

  /*
  the request cannot be processed because the
                coordinator node is overloaded

  */
  val OVERLOADED       = 0x1001

  
  
  /*the request was a read request but the
              coordinator node is bootstrapping
              * 
              */
  val IS_BOOTSTRAPPING = 0x1002
  
/*error during a truncation error.
 * 
 */
  val TRUNCATE_ERROR   = 0x1003
  
/* Timeout exception during a write request. The rest
              of the ERROR message body will be
                <cl><received><blockfor><writeType>
              where:
                <cl> is the [consistency] level of the query having triggered
                     the exception.
                <received> is an [int] representing the number of nodes having
                           acknowledged the request.
                <blockfor> is the number of replica whose acknowledgement is
                           required to achieve <cl>.
                <writeType> is a [string] that describe the type of the write
                            that timeouted. The value of that string can be one
                            of:
                             - "SIMPLE": the write was a non-batched
                               non-counter write.
                             - "BATCH": the write was a (logged) batch write.
                               If this type is received, it means the batch log
                               has been successfully written (otherwise a
                               "BATCH_LOG" type would have been send instead).
                             - "UNLOGGED_BATCH": the write was an unlogged
                               batch. Not batch log write has been attempted.
                             - "COUNTER": the write was a counter write
                               (batched or not).
                             - "BATCH_LOG": the timeout occured during the
                               write to the batch log when a (logged) batch
                               write was requested.
                               * 
                               */
  val WRITE_TIMEOUT    = 0x1100
  
/* Timeout exception during a read request. The rest
              of the ERROR message body will be
                <cl><received><blockfor><data_present>
              where:
                <cl> is the [consistency] level of the query having triggered
                     the exception.
                <received> is an [int] representing the number of nodes having
                           answered the request.
                <blockfor> is the number of replica whose response is
                           required to achieve <cl>. Please note that it is
                           possible to have <received> >= <blockfor> if
                           <data_present> is false. And also in the (unlikely)
                           case were <cl> is achieved but the coordinator node
                           timeout while waiting for read-repair
                           acknowledgement.
                <data_present> is a single byte. If its value is 0, it means
                               the replica that was asked for data has not
                               responded. Otherwise, the value is != 0.
                               * 
                               */
  val READ_TIMEOUT     = 0x1200
  
/* The submitted query has a syntax error.
 * 
 */
  val SYNTAX_ERROR     = 0x2000

  /* The logged user doesn't have the right to perform
              the query.
              * 
              */
  val UNAUTHORIZED     = 0x2100

  /* The query is syntactically correct but invalid.
   * 
   */
  val INVALID          = 0x2200
  
/* The query is invalid because of some configuration issue
 * 
 */
  val CONFIG_ERROR     = 0x2300
  
/* The query attempted to create a keyspace or a
              table that was already existing. The rest of the ERROR message
              body will be <ks><table> where:
                <ks> is a [string] representing either the keyspace that
                     already exists, or the keyspace in which the table that
                     already exists is.
                <table> is a [string] representing the name of the table that
                        already exists. If the query was attempting to create a
                        keyspace, <table> will be present but will be the empty string.
                        * 
                        */
  val ALREADY_EXISTS   = 0x2400
  
    /* Can be thrown while a prepared statement tries to be
              executed if the provide prepared statement ID is not known by
              this host. The rest of the ERROR message body will be [short
              bytes] representing the unknown ID.
              * 
              */
  val UNPREPARED       = 0x2500

  /*********************************************************************/


  // Consistency Levels
  val ANY: Short         =										0x0000
  val ONE: Short         =										0x0001
  val TWO: Short          =										0x0002
  val THREE: Short        =										0x0003
  val QUORUM : Short      =										0x0004
  val ALL: Short          =										0x0005
  val LOCAL_QUORUM: Short =										0x0006
  val EACH_QUORUM: Short  =										0x0007
  val LOCAL_ONE: Short    =										0x000A

  // Kinds of result
  val VOID: Int          = 0x0001    // for results carrying no information.
  val ROWS: Int          = 0x0002    // for results to select queries, returning a set of rows.
  val SET_KEYSPACE: Int  = 0x0003    // the result to a `use` query.
  val PREPARED: Int      = 0x0004    // result to a PREPARE message.
  val SCHEMA_CHANGE: Int = 0x0005    // the result to a schema altering query.

}
