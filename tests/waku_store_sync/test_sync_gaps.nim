{.used.}

## A relay message the archive refuses (here because it arrived late) must stay out of the
## sync index. Otherwise the node believes it holds the message and never asks a peer for
## it, which left rows missing on one store node in status.prod on 2026-10-22.

import results, testutils/unittests, chronos, chronicles
import
  ../../logos_delivery/waku/[
    node/peer_manager,
    waku_core,
    waku_core/message,
    waku_core/message/digest,
    waku_store_sync/common,
    waku_store_sync/reconciliation,
    waku_store_sync/transfer,
    waku_archive/archive,
    waku_archive/driver,
    waku_archive/common,
  ],
  ../testlib/[wakucore, testasync],
  ../waku_archive/archive_utils

type SyncNode = object
  switch: Switch
  peerManager: PeerManager
  archive: WakuArchive
  driver: ArchiveDriver
  recon: SyncReconciliation
  transfer: SyncTransfer
  info: RemotePeerInfo

proc newSyncNode(): Future[SyncNode] {.async.} =
  let
    switch = newTestSwitch()
    driver = newSqliteArchiveDriver()
    # no retention policy: the default one would clamp the sync range
    archive = WakuArchive.new(driver, retentionPolicies = @[]).get()

  await switch.start()

  let
    peerManager = PeerManager.new(switch)
    ids = newAsyncQueue[(SyncID, PubsubTopic, ContentTopic)]()
    wants = newAsyncQueue[PeerId]()
    needs = newAsyncQueue[(PeerId, WakuMessageHash)]()
    recon = (
      await SyncReconciliation.new(
        cluster = DefaultClusterId,
        shards = @[],
        peerManager = peerManager,
        wakuArchive = archive,
        syncRange = DefaultSyncRange,
        relayJitter = 0.seconds,
        idsRx = ids,
        localWantsTx = wants,
        remoteNeedsTx = needs,
      )
    ).get()
    transfer = SyncTransfer.new(
      peerManager, archive, ids, wants, needs, int(DefaultMaxWakuMessageSize)
    )

  await recon.start()
  await transfer.start()

  switch.mount(recon)
  switch.mount(transfer)

  return SyncNode(
    switch: switch,
    peerManager: peerManager,
    archive: archive,
    driver: driver,
    recon: recon,
    transfer: transfer,
    info: switch.peerInfo.toRemotePeerInfo(),
  )

proc stop(n: SyncNode) {.async.} =
  await n.transfer.stop()
  await n.recon.stop()
  await n.switch.stop()

proc holds(n: SyncNode, msg: WakuMessage): Future[bool] {.async.} =
  return await n.archive.holdsMessages(@[computeMessageHash(DefaultPubsubTopic, msg)])

proc holdsAll(n: SyncNode, msgs: seq[WakuMessage]): Future[bool] {.async.} =
  for m in msgs:
    if not (await n.holds(m)):
      return false
  return true

suite "Waku Archive: storeMessage":
  var archive {.threadvar.}: WakuArchive

  setup:
    archive = newWakuArchive(newSqliteArchiveDriver())

  asyncTest "a message in the accepted time window is stored":
    check (await archive.storeMessage(DefaultPubsubTopic, fakeWakuMessage())).isOk()

  asyncTest "a message that is already stored is a success":
    let msg = fakeWakuMessage()

    check:
      (await archive.storeMessage(DefaultPubsubTopic, msg)).isOk()
      (await archive.storeMessage(DefaultPubsubTopic, msg)).isOk()

  asyncTest "a message with an old timestamp is refused and not stored":
    let msg = fakeWakuMessage(ts = now() - 60_000_000_000)

    check:
      (await archive.storeMessage(DefaultPubsubTopic, msg)).isErr()
      not (await archive.holdsMessages(@[computeMessageHash(DefaultPubsubTopic, msg)]))

suite "Waku Sync: relay messages refused by the archive":
  var
    a {.threadvar.}: SyncNode
    b {.threadvar.}: SyncNode

  asyncSetup:
    a = await newSyncNode()
    b = await newSyncNode()
    a.peerManager.addPeer(b.info)
    b.peerManager.addPeer(a.info)

  asyncTeardown:
    await allFutures(a.stop(), b.stop())

  asyncTest "a late relay message that the archive refused is synced":
    let
      ts = now() - 60_000_000_000
      stale = fakeWakuMessage(ts = ts)
      control = fakeWakuMessage(ts = ts + 1)

    discard await a.driver.put(DefaultPubsubTopic, @[stale, control])
    a.recon.messageIngress(DefaultPubsubTopic, stale)
    a.recon.messageIngress(DefaultPubsubTopic, control)

    # what b does when the relay delivers the message 60 s late: the archive refuses it,
    # so the relay handler must not add it to the sync index
    check:
      (await b.archive.storeMessage(DefaultPubsubTopic, stale)).isErr()
      not (await b.holds(stale))

    (await b.recon.storeSynchronization(Opt.some(a.info))).isOkOr:
      assert false, error

    checkUntilTimeout:
      await b.holdsAll(@[stale, control])
