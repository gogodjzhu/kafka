/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.cluster

import java.io.IOException
import java.util.concurrent.locks.ReentrantReadWriteLock

import com.yammer.metrics.core.Gauge
import kafka.admin.AdminUtils
import kafka.api.LeaderAndIsr
import kafka.common.NotAssignedReplicaException
import kafka.controller.KafkaController
import kafka.log.LogConfig
import kafka.metrics.KafkaMetricsGroup
import kafka.server._
import kafka.utils.CoreUtils.{inReadLock, inWriteLock}
import kafka.utils._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{NotEnoughReplicasException, NotLeaderForPartitionException, PolicyViolationException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.protocol.Errors._
import org.apache.kafka.common.record.MemoryRecords
import org.apache.kafka.common.requests.EpochEndOffset._
import org.apache.kafka.common.requests.{EpochEndOffset, PartitionState}
import org.apache.kafka.common.utils.Time

import scala.collection.JavaConverters._

/**
 * Data structure that represents a topic partition. The leader maintains the AR, ISR, CUR, RAR
 */
class Partition(val topic: String,
                val partitionId: Int,
                time: Time,
                replicaManager: ReplicaManager) extends Logging with KafkaMetricsGroup {
  val topicPartition = new TopicPartition(topic, partitionId)

  private val localBrokerId = replicaManager.config.brokerId
  private val logManager = replicaManager.logManager
  private val zkUtils = replicaManager.zkUtils
  // partition在replica上的分布, replicaId -> Replica
  private val assignedReplicaMap = new Pool[Int, Replica]
  // The read lock is only required when multiple reads are executed and needs to be in a consistent manner
  private val leaderIsrUpdateLock = new ReentrantReadWriteLock
  private var zkVersion: Int = LeaderAndIsr.initialZKVersion
  @volatile private var leaderEpoch: Int = LeaderAndIsr.initialLeaderEpoch - 1
  @volatile var leaderReplicaIdOpt: Option[Int] = None
  // ISR列表
  @volatile var inSyncReplicas: Set[Replica] = Set.empty[Replica]

  /* Epoch of the controller that last changed the leader. This needs to be initialized correctly upon broker startup.
   * One way of doing that is through the controller's start replica state change command. When a new broker starts up
   * the controller sends it a start replica command containing the leader for each partition that the broker hosts.
   * In addition to the leader, the controller can also send the epoch of the controller that elected the leader for
   * each partition. */
  private var controllerEpoch: Int = KafkaController.InitialControllerEpoch - 1
  this.logIdent = "Partition [%s,%d] on broker %d: ".format(topic, partitionId, localBrokerId)

  private def isReplicaLocal(replicaId: Int) : Boolean = replicaId == localBrokerId
  val tags = Map("topic" -> topic, "partition" -> partitionId.toString)

  newGauge("UnderReplicated",
    new Gauge[Int] {
      def value = {
        if (isUnderReplicated) 1 else 0
      }
    },
    tags
  )

  newGauge("InSyncReplicasCount",
    new Gauge[Int] {
      def value = {
        if (isLeaderReplicaLocal) inSyncReplicas.size else 0
      }
    },
    tags
  )

  newGauge("ReplicasCount",
    new Gauge[Int] {
      def value = {
        if (isLeaderReplicaLocal) assignedReplicas.size else 0
      }
    },
    tags
  )

  newGauge("LastStableOffsetLag",
    new Gauge[Long] {
      def value = {
        leaderReplicaIfLocal.map { replica =>
          replica.highWatermark.messageOffset - replica.lastStableOffset.messageOffset
        }.getOrElse(0)
      }
    },
    tags
  )

  private def isLeaderReplicaLocal: Boolean = leaderReplicaIfLocal.isDefined

  def isUnderReplicated: Boolean =
    isLeaderReplicaLocal && inSyncReplicas.size < assignedReplicas.size

  // 在指定broker(replicaId)上创建当前partition实例的备份
  def getOrCreateReplica(replicaId: Int = localBrokerId): Replica = {
    // 若本地缓存中已经存在replica, 即原本作为partition的replica未变化, 直接返回此replica
    // 否则新建新的replica对象来保存备份partition
    assignedReplicaMap.getAndMaybePut(replicaId, {
      if (isReplicaLocal(replicaId)) { // replica落在本broker, 指定replica就是当前replica(broker)
        // 配置
        val config = LogConfig.fromProps(logManager.defaultConfig.originals,
                                         AdminUtils.fetchEntityConfig(zkUtils, ConfigType.Topic, topic))
        // 从logManager获取对应TopicPartition的Log对象,
        // 若已存在对应TopicPartition的Log对象(在LogManager#loadLogs方法中扫描目录得到), 则直接返回
        // 否则当前broker是第一次作为此TopicPartition的replica, 需要创建对应目录并返回Log对象
        val log = logManager.createLog(topicPartition, config)

        /* 恢复当前replica的HW offset */
        val checkpoint = replicaManager.highWatermarkCheckpoints(log.dir.getParentFile.getAbsolutePath)
        val offsetMap = checkpoint.read
        if (!offsetMap.contains(topicPartition))
          info(s"No checkpointed highwatermark is found for partition $topicPartition")
        val offset = math.min(offsetMap.getOrElse(topicPartition, 0L), log.logEndOffset)
        new Replica(replicaId, topicPartition, time, offset, Some(log)) // 返回的replica对象包含offset和log对象, 这是与非本地replica的区别
      }
      else new Replica(replicaId, topicPartition, time)
    })
  }

  def getReplica(replicaId: Int = localBrokerId): Option[Replica] = Option(assignedReplicaMap.get(replicaId))

  def leaderReplicaIfLocal: Option[Replica] =
    leaderReplicaIdOpt.filter(_ == localBrokerId).flatMap(getReplica)

  def addReplicaIfNotExists(replica: Replica): Replica =
    assignedReplicaMap.putIfNotExists(replica.brokerId, replica)

  def assignedReplicas: Set[Replica] =
    assignedReplicaMap.values.toSet

  private def removeReplica(replicaId: Int) {
    assignedReplicaMap.remove(replicaId)
  }

  def delete() {
    // need to hold the lock to prevent appendMessagesToLeader() from hitting I/O exceptions due to log being deleted
    inWriteLock(leaderIsrUpdateLock) {
      assignedReplicaMap.clear()
      inSyncReplicas = Set.empty[Replica]
      leaderReplicaIdOpt = None
      try {
        logManager.asyncDelete(topicPartition)
        removePartitionMetrics()
      } catch {
        case e: IOException =>
          fatal(s"Error deleting the log for partition $topicPartition", e)
          Exit.halt(1)
      }
    }
  }

  def getLeaderEpoch: Int = this.leaderEpoch

  /**
   * Make the local replica the leader by resetting LogEndOffset for remote replicas (there could be old LogEndOffset
   * from the time when this broker was the leader last time) and setting the new leader and ISR.
   * If the leader replica id does not change, return false to indicate the replica manager.
   */
  def makeLeader(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    val (leaderHWIncremented, isNewLeader) = inWriteLock(leaderIsrUpdateLock) {
      // 信息来自controller从zk读取的replica存活状态, 因此认为这里的allReplicas是当前集群存活的所有为此partition服务的replica
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      // 创建新replica 或 从缓存获取指定replica
      val newInSyncReplicas = partitionStateInfo.isr.asScala.map(r => getOrCreateReplica(r)).toSet
      // remove assigned replicas that have been removed by the controller
      // 从本地缓存assignedReplicas剔除当前为此 controller给出的仍在给此partition服务的replica剩下的即为非本partition的replica
      // (曾经可能是，但controller给出的最新状态中被剔除了)，这里将其从缓存中清除
      (assignedReplicas.map(_.brokerId) -- allReplicas).foreach(removeReplica)
      // 更新isr缓存
      inSyncReplicas = newInSyncReplicas

      info(s"$topicPartition starts at Leader Epoch ${partitionStateInfo.leaderEpoch} from offset ${getReplica().get.logEndOffset.messageOffset}. Previous Leader Epoch was: $leaderEpoch")

      //We cache the leader epoch here, persisting it only if it's local (hence having a log dir)
      leaderEpoch = partitionStateInfo.leaderEpoch
      allReplicas.foreach(id => getOrCreateReplica(id))

      zkVersion = partitionStateInfo.zkVersion
      // 是否为新晋leader
      val isNewLeader =
        if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == localBrokerId) {
          false
        } else {
          leaderReplicaIdOpt = Some(localBrokerId)
          true
        }
      val leaderReplica = getReplica().get
      // 当前leaderReplica的LEO
      val curLeaderLogEndOffset = leaderReplica.logEndOffset.messageOffset
      val curTimeMs = time.milliseconds
      // initialize lastCaughtUpTime of replicas as well as their lastFetchTimeMs and lastFetchLeaderLogEndOffset.
      (assignedReplicas - leaderReplica).foreach { replica =>
        val lastCaughtUpTimeMs = if (inSyncReplicas.contains(replica)) curTimeMs else 0L
        replica.resetLastCaughtUpTime(curLeaderLogEndOffset, curTimeMs, lastCaughtUpTimeMs)
      }
      // we may need to increment high watermark since ISR could be down to 1
      if (isNewLeader) {
        // construct the high watermark metadata for the new leader replica
        leaderReplica.convertHWToLocalOffsetMetadata()
        // reset log end offset for remote replicas
        assignedReplicas.filter(_.brokerId != localBrokerId).foreach(_.updateLogReadResult(LogReadResult.UnknownLogReadResult))
      }
      (maybeIncrementLeaderHW(leaderReplica), isNewLeader)
    }
    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
    isNewLeader
  }

  /**
   *  Make the local replica the follower by setting the new leader and ISR to empty
   *  If the leader replica id does not change, return false to indicate the replica manager
   *  把当前replica设置为指定partition的follower, 并且清空ISR列表(follower不需要ISR列表)
   *  如果切换的leaderId未变, 放弃此次修改, 返回false
   */
  def makeFollower(controllerId: Int, partitionStateInfo: PartitionState, correlationId: Int): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      val allReplicas = partitionStateInfo.replicas.asScala.map(_.toInt)
      val newLeaderBrokerId: Int = partitionStateInfo.leader
      // record the epoch of the controller that made the leadership decision. This is useful while updating the isr
      // to maintain the decision maker controller's epoch in the zookeeper path
      controllerEpoch = partitionStateInfo.controllerEpoch
      // add replicas that are new
      allReplicas.foreach(r => getOrCreateReplica(r))
      // remove assigned replicas that have been removed by the controller
      (assignedReplicas.map(_.brokerId) -- allReplicas).foreach(removeReplica)
      inSyncReplicas = Set.empty[Replica] // makeFollower, 清空ISR列表(follower不需要)
      leaderEpoch = partitionStateInfo.leaderEpoch
      zkVersion = partitionStateInfo.zkVersion

      if (leaderReplicaIdOpt.isDefined && leaderReplicaIdOpt.get == newLeaderBrokerId) {
        false
      }
      else {
        leaderReplicaIdOpt = Some(newLeaderBrokerId)
        true
      }
    }
  }

  /**
   * Update the log end offset of a certain replica of this partition
   */
  def updateReplicaLogReadResult(replicaId: Int, logReadResult: LogReadResult) {
    // 拉取指定replicaId的副本的信息
    getReplica(replicaId) match {
      case Some(replica) =>
        // No need to calculate low watermark if there is no delayed DeleteRecordsRequest
        val oldLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L
        replica.updateLogReadResult(logReadResult) // 更新replica的logOffset, 这可能会影响lowWatermarkIfLeader方法的返回(即log水位变动)
        val newLeaderLW = if (replicaManager.delayedDeleteRecordsPurgatory.delayed > 0) lowWatermarkIfLeader else -1L
        // check if the LW of the partition has incremented
        // since the replica's logStartOffset may have incremented
        val leaderLWIncremented = newLeaderLW > oldLeaderLW
        // check if we need to expand ISR to include this replica
        // if it is not in the ISR yet
        val leaderHWIncremented = maybeExpandIsr(replicaId, logReadResult)

        // some delayed operations may be unblocked after HW or LW changed
        if (leaderLWIncremented || leaderHWIncremented)
          tryCompleteDelayedRequests()

        debug("Recorded replica %d log end offset (LEO) position %d."
          .format(replicaId, logReadResult.info.fetchOffsetMetadata.messageOffset))
      case None =>
        throw new NotAssignedReplicaException(("Leader %d failed to record follower %d's position %d since the replica" +
          " is not recognized to be one of the assigned replicas %s for partition %s.")
          .format(localBrokerId,
                  replicaId,
                  logReadResult.info.fetchOffsetMetadata.messageOffset,
                  assignedReplicas.map(_.brokerId).mkString(","),
                  topicPartition))
    }
  }

  /**
   * Check and maybe expand the ISR of the partition.
   * A replica will be added to ISR if its LEO >= current hw of the partition.
   *
   * Technically, a replica shouldn't be in ISR if it hasn't caught up for longer than replicaLagTimeMaxMs,
   * even if its log end offset is >= HW. However, to be consistent with how the follower determines
   * whether a replica is in-sync, we only check HW.
   *
   * This function can be triggered when a replica's LEO has incremented
   *
   * 检查更新Isr列表
   * 一个replica进入ISR的条件是: 此replica的LogEndOffset(LEO) >= 分区的高水位(HW)
   * 准确地说, ISR的判定有两个标准:
   * 1. follower的LEO是否追上leader的LEO (replica.lag.max.messages)[存在的问题是如果瞬时消息生成比消费快就会导致out Sync]
   * 2. follower在近期发送过FetchRequest请求给Leader (replica.lag.time.max.ms)[存在的问题是如果follower频繁拉取少量数据, 会误判为inSync]
   * 在0.9.0.0的更新中移除了replica.lag.max.messages配置, 将两个标准合并为一个:
   * 最近一次追上leader的时间, 低于replica.lag.time.max.ms时认为inSync
   */
  def maybeExpandIsr(replicaId: Int, logReadResult: LogReadResult): Boolean = {
    inWriteLock(leaderIsrUpdateLock) {
      // check if this replica needs to be added to the ISR
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val replica = getReplica(replicaId).get
          val leaderHW = leaderReplica.highWatermark
          if(!inSyncReplicas.contains(replica) && // 当前ISR中没有指定replica
             assignedReplicas.map(_.brokerId).contains(replicaId) && // 指定replica在partition的assign列表
             replica.logEndOffset.offsetDiff(leaderHW) >= 0) { // 指定replica的LEO大于等于partition的HW
            val newInSyncReplicas = inSyncReplicas + replica // 满足以上条件, 这个replica可以进入ISR
            info(s"Expanding ISR from ${inSyncReplicas.map(_.brokerId).mkString(",")} " +
              s"to ${newInSyncReplicas.map(_.brokerId).mkString(",")}")
            // update ISR in ZK and cache
            updateIsr(newInSyncReplicas) // 在zk中更新ISR, ZK更新成功后将信息写入本地缓存inSyncReplicas
            replicaManager.isrExpandRate.mark()
          }
          // check if the HW of the partition can now be incremented
          // since the replica may already be in the ISR and its LEO has just incremented
          maybeIncrementLeaderHW(leaderReplica, logReadResult.fetchTimeMs)
        case None => false // nothing to do if no longer leader
      }
    }
  }

  /*
   * Returns a tuple where the first element is a boolean indicating whether enough replicas reached `requiredOffset`
   * and the second element is an error (which would be `Errors.NONE` for no error).
   *
   * Note that this method will only be called if requiredAcks = -1 and we are waiting for all replicas in ISR to be
   * fully caught up to the (local) leader's offset corresponding to this produce request before we acknowledge the
   * produce request.
   */
  def checkEnoughReplicasReachOffset(requiredOffset: Long): (Boolean, Errors) = {
    leaderReplicaIfLocal match {
      case Some(leaderReplica) =>
        // keep the current immutable replica list reference
        val curInSyncReplicas = inSyncReplicas

        def numAcks = curInSyncReplicas.count { r =>
          if (!r.isLocal)
            if (r.logEndOffset.messageOffset >= requiredOffset) {
              trace(s"Replica ${r.brokerId} received offset $requiredOffset")
              true
            }
            else
              false
          else
            true /* also count the local (leader) replica */
        }

        trace(s"$numAcks acks satisfied with acks = -1")

        val minIsr = leaderReplica.log.get.config.minInSyncReplicas

        if (leaderReplica.highWatermark.messageOffset >= requiredOffset) {
          /*
           * The topic may be configured not to accept messages if there are not enough replicas in ISR
           * in this scenario the request was already appended locally and then added to the purgatory before the ISR was shrunk
           */
          if (minIsr <= curInSyncReplicas.size)
            (true, Errors.NONE)
          else
            (true, Errors.NOT_ENOUGH_REPLICAS_AFTER_APPEND)
        } else
          (false, Errors.NONE)
      case None =>
        (false, Errors.NOT_LEADER_FOR_PARTITION)
    }
  }

  /**
   * Check and maybe increment the high watermark of the partition;
   * this function can be triggered when
   *
   * 1. Partition ISR changed
   * 2. Any replica's LEO changed
   *
   * The HW is determined by the smallest log end offset among all replicas that are in sync or are considered caught-up.
   * This way, if a replica is considered caught-up, but its log end offset is smaller than HW, we will wait for this
   * replica to catch up to the HW before advancing the HW. This helps the situation when the ISR only includes the
   * leader replica and a follower tries to catch up. If we don't wait for the follower when advancing the HW, the
   * follower's log end offset may keep falling behind the HW (determined by the leader's log end offset) and therefore
   * will never be added to ISR.
   *
   * Returns true if the HW was incremented, and false otherwise.
   * Note There is no need to acquire the leaderIsrUpdate lock here
   * since all callers of this private API acquire that lock
   */
  private def maybeIncrementLeaderHW(leaderReplica: Replica, curTime: Long = time.milliseconds): Boolean = {
    val allLogEndOffsets = assignedReplicas.filter { replica =>
      curTime - replica.lastCaughtUpTimeMs <= replicaManager.config.replicaLagTimeMaxMs || inSyncReplicas.contains(replica)
    }.map(_.logEndOffset)
    val newHighWatermark = allLogEndOffsets.min(new LogOffsetMetadata.OffsetOrdering)
    val oldHighWatermark = leaderReplica.highWatermark
    if (oldHighWatermark.messageOffset < newHighWatermark.messageOffset || oldHighWatermark.onOlderSegment(newHighWatermark)) {
      // highWatermark is volatile
      leaderReplica.highWatermark = newHighWatermark
      debug(s"High watermark updated to $newHighWatermark")
      true
    } else  {
      debug(s"Skipping update high watermark since new hw $newHighWatermark is not larger than old hw $oldHighWatermark." +
        s"All LEOs are ${allLogEndOffsets.mkString(",")}")
      false
    }
  }

  /**
   * The low watermark offset value, calculated only if the local replica is the partition leader
   * It is only used by leader broker to decide when DeleteRecordsRequest is satisfied. Its value is minimum logStartOffset of all live replicas
   * Low watermark will increase when the leader broker receives either FetchRequest or DeleteRecordsRequest.
   * 仅供Partition Leader实例使用, 获取LW offset
   * 低水位(LW)的定义为: 所有存活的replica中的LogStartOffset最小值
   */
  def lowWatermarkIfLeader: Long = {
    if (!isLeaderReplicaLocal)
      throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d".format(topicPartition, localBrokerId))
    assignedReplicas.filter(replica =>
      replicaManager.metadataCache.isBrokerAlive(replica.brokerId)).map(_.logStartOffset).reduceOption(_ min _).getOrElse(0L)
  }

  /**
   * Try to complete any pending requests. This should be called without holding the leaderIsrUpdateLock.
   */
  private def tryCompleteDelayedRequests() {
    val requestKey = new TopicPartitionOperationKey(topicPartition)
    replicaManager.tryCompleteDelayedFetch(requestKey)
    replicaManager.tryCompleteDelayedProduce(requestKey)
    replicaManager.tryCompleteDelayedDeleteRecords(requestKey)
  }

  /**
   * 修剪ISR列表
   */
  def maybeShrinkIsr(replicaMaxLagTimeMs: Long) {
    val leaderHWIncremented = inWriteLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val outOfSyncReplicas = getOutOfSyncReplicas(leaderReplica, replicaMaxLagTimeMs)
          if(outOfSyncReplicas.nonEmpty) {
            val newInSyncReplicas = inSyncReplicas -- outOfSyncReplicas // maybeShrinkIsr
            assert(newInSyncReplicas.nonEmpty)
            info("Shrinking ISR from %s to %s".format(inSyncReplicas.map(_.brokerId).mkString(","),
              newInSyncReplicas.map(_.brokerId).mkString(",")))
            // update ISR in zk and in cache
            updateIsr(newInSyncReplicas)
            // we may need to increment high watermark since ISR could be down to 1

            replicaManager.isrShrinkRate.mark()
            maybeIncrementLeaderHW(leaderReplica)
          } else {
            false
          }

        case None => false // do nothing if no longer leader
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()
  }

  def getOutOfSyncReplicas(leaderReplica: Replica, maxLagMs: Long): Set[Replica] = {
    /**
     * there are two cases that will be handled here -
     * 1. Stuck followers: If the leo of the replica hasn't been updated for maxLagMs ms,
     *                     the follower is stuck and should be removed from the ISR
     * 2. Slow followers: If the replica has not read up to the leo within the last maxLagMs ms,
     *                    then the follower is lagging and should be removed from the ISR
     * Both these cases are handled by checking the lastCaughtUpTimeMs which represents
     * the last time when the replica was fully caught up. If either of the above conditions
     * is violated, that replica is considered to be out of sync
     *
     * 存在两种需要被移出ISR的follower:
     * - 阻塞Follower: 超过maxLagMs的时间此replica 的LEO未更新
     * - 慢Follower: 超过maxLogMs的时间此replica未追上leader LEO
     * 这里将两者合成一种判断: 最近replica追上leader LEO的时间若大于maxLogMs就视为outSync
     **/
    val candidateReplicas = inSyncReplicas - leaderReplica // getOutOfSyncReplicas

    val laggingReplicas = candidateReplicas.filter(r => (time.milliseconds - r.lastCaughtUpTimeMs) > maxLagMs)
    if (laggingReplicas.nonEmpty)
      debug("Lagging replicas are %s".format(laggingReplicas.map(_.brokerId).mkString(",")))

    laggingReplicas
  }

  def appendRecordsToLeader(records: MemoryRecords, isFromClient: Boolean, requiredAcks: Int = 0) = {
    val (info, leaderHWIncremented) = inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          val log = leaderReplica.log.get
          // minInSyncReplicas指定了当ack配置为all或者-1的时候，leader需要等到多少个isr的replica(包括leader自身)响应才认为写入成功
          val minIsr = log.config.minInSyncReplicas
          val inSyncSize = inSyncReplicas.size

          // Avoid writing to leader if there are not enough insync replicas to make it safe
          if (inSyncSize < minIsr && requiredAcks == -1) {
            // isr状态的replica不足minIsr，抛出异常
            // PS. 如果手动配置minInSyncReplicas，需保证minInSyncReplicas小于或等于正常运行时的实际replica数量。否则永远无法成功发送
            throw new NotEnoughReplicasException("Number of insync replicas for partition %s is [%d], below required minimum [%d]"
              .format(topicPartition, inSyncSize, minIsr))
          }

          val info = log.appendAsLeader(records, leaderEpoch = this.leaderEpoch, isFromClient)
          // probably unblock some follower fetch requests since log end offset has been updated
          replicaManager.tryCompleteDelayedFetch(TopicPartitionOperationKey(this.topic, this.partitionId))
          // we may need to increment high watermark since ISR could be down to 1
          (info, maybeIncrementLeaderHW(leaderReplica))

        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }

    // some delayed operations may be unblocked after HW changed
    if (leaderHWIncremented)
      tryCompleteDelayedRequests()

    info
  }

  /**
   * Update logStartOffset and low watermark if 1) offset <= highWatermark and 2) it is the leader replica.
   * This function can trigger log segment deletion and log rolling.
   *
   * Return low watermark of the partition.
   */
  def deleteRecordsOnLeader(offset: Long): Long = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          leaderReplica.maybeIncrementLogStartOffset(offset)
          if (!leaderReplica.log.get.config.delete)
            throw new PolicyViolationException("Records of partition %s can not be deleted due to the configured policy".format(topicPartition))
          lowWatermarkIfLeader
        case None =>
          throw new NotLeaderForPartitionException("Leader not local for partition %s on broker %d"
            .format(topicPartition, localBrokerId))
      }
    }
  }

  /**
    * @param leaderEpoch Requested leader epoch
    * @return The last offset of messages published under this leader epoch.
    */
  def lastOffsetForLeaderEpoch(leaderEpoch: Int): EpochEndOffset = {
    inReadLock(leaderIsrUpdateLock) {
      leaderReplicaIfLocal match {
        case Some(leaderReplica) =>
          new EpochEndOffset(NONE, leaderReplica.epochs.get.endOffsetFor(leaderEpoch))
        case None =>
          new EpochEndOffset(NOT_LEADER_FOR_PARTITION, UNDEFINED_EPOCH_OFFSET)
      }
    }
  }

  private def updateIsr(newIsr: Set[Replica]) {
    val newLeaderAndIsr = new LeaderAndIsr(localBrokerId, leaderEpoch, newIsr.map(_.brokerId).toList, zkVersion)
    val (updateSucceeded,newVersion) = ReplicationUtils.updateLeaderAndIsr(zkUtils, topic, partitionId,
      newLeaderAndIsr, controllerEpoch, zkVersion)

    if(updateSucceeded) {
      replicaManager.recordIsrChange(topicPartition)
      inSyncReplicas = newIsr
      zkVersion = newVersion
      trace("ISR updated to [%s] and zkVersion updated to [%d]".format(newIsr.mkString(","), zkVersion))
    } else {
      replicaManager.failedIsrUpdatesRate.mark()
      info("Cached zkVersion [%d] not equal to that in zookeeper, skip updating ISR".format(zkVersion))
    }
  }

  /**
   * remove deleted log metrics
   */
  private def removePartitionMetrics() {
    removeMetric("UnderReplicated", tags)
    removeMetric("InSyncReplicasCount", tags)
    removeMetric("ReplicasCount", tags)
    removeMetric("LastStableOffsetLag", tags)
  }

  override def equals(that: Any): Boolean = that match {
    case other: Partition => partitionId == other.partitionId && topic == other.topic
    case _ => false
  }

  override def hashCode: Int =
    31 + topic.hashCode + 17 * partitionId

  override def toString: String = {
    val partitionString = new StringBuilder
    partitionString.append("Topic: " + topic)
    partitionString.append("; Partition: " + partitionId)
    partitionString.append("; Leader: " + leaderReplicaIdOpt)
    partitionString.append("; AssignedReplicas: " + assignedReplicaMap.keys.mkString(","))
    partitionString.append("; InSyncReplicas: " + inSyncReplicas.map(_.brokerId).mkString(","))
    partitionString.toString
  }
}
