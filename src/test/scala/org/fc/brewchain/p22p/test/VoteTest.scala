package org.fc.brewchain.p22p.test

import org.fc.brewchain.p22p.core.Votes._
//import scala.collection.Searching._

object VoteTest {
  def main(args: Array[String]): Unit = {
    val l = List((1,"aa"),(0,"bb"),(1,"bb"),(1,"bb"),(1,"bb"))
    println("pbft.vote=" + l.PBFTVote(f =>Some(f),5));
    println("rcpt.vote=" + l.RCPTVote().decision);
    println("rcpt.vote=" + l.precentVote(0.6F).decision);
  }

}