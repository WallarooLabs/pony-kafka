class MockTCPConnectionHandler is TCPConnectionHandler
  new create()
  =>
    None

  fun ref write(data: ByteSeq) =>
    """
    Write a single sequence of bytes.
    """
    None

  fun ref queue(data: ByteSeq) =>
    """
    Queue a single sequence of bytes on linux.
    Do nothing on windows.
    """
    None

  fun ref writev(data: ByteSeqIter) =>
    """
    Write a sequence of sequences of bytes.
    """
    None

  fun ref queuev(data: ByteSeqIter) =>
    """
    Queue a sequence of sequences of bytes on linux.
    Do nothing on windows.
    """
    None

  fun ref send_queue() =>
    """
    Write pending queue to network on linux.
    Do nothing on windows.
    """
    None

  fun ref mute(d: Any tag) =>
    """
    Temporarily suspend reading off this TCPConnection until such time as
    `unmute` is called.
    """
    None

  fun ref unmute(d: Any tag) =>
    """
    Start reading off this TCPConnection again after having been muted.
    """
    None

  fun ref set_notify(notify: CustomTCPConnectionNotify iso) =>
    """
    Change the notifier.
    """
    None

  fun ref dispose() =>
    """
    Close the connection gracefully once all writes are sent.
    """
    None

  fun ref event_notify(event: AsioEventID, flags: U32, arg: U32) =>
    """
    Handle socket events.
    """
    None

  fun ref pending_reads() =>
    """
    Unless this connection is currently muted, read while data is available,
    guessing the next packet length as we go. If we read 4 kb of data, send
    ourself a resume message and stop reading, to avoid starving other actors.
    """
    None

  fun ref expect(qty: USize = 0) =>
    """
    A `received` call on the notifier must contain exactly `qty` bytes. If
    `qty` is zero, the call can contain any amount of data. This has no effect
    if called in the `sent` notifier callback.
    """
    None

  fun ref reconnect() =>
    None
