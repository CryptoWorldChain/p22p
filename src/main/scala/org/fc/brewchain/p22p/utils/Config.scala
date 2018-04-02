package org.fc.brewchain.p22p.utils

import onight.tfw.mservice.NodeHelper

object Config {
  def TIMEOUT_STATE_VIEW = 60 * 1000;
  def TIMEOUT_STATE_VIEW_RESET = 360 * 1000;
  def MIN_EPOCH_EACH_VOTE = 10
  def MAX_VOTE_SLEEP_MS = 60000;
  def MIN_VOTE_SLEEP_MS = 10000;
  
  def TICK_CHECK_HEALTHY = 10;
  def TICK_VOTE_MAP = 10; 
  def TICK_VOTE_WORKER = 1; 
  
  def VOTE_DEBUG:Boolean = {
//    NodeHelper.getCurrNodeListenOutPort != 5100;
    false
  }
}