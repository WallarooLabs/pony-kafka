use "net"

trait CustomTCPConnection
  fun ref get_handler(): TCPConnectionHandler

  be write(data: ByteSeq) =>
    """
    Write a single sequence of bytes.
    """
    get_handler().write(data)

  be queue(data: ByteSeq) =>
    """
    Queue a single sequence of bytes on linux.
    Do nothing on windows.
    """
    get_handler().queue(data)

  be writev(data: ByteSeqIter) =>
    """
    Write a sequence of sequences of bytes.
    """
    get_handler().writev(data)

  be queuev(data: ByteSeqIter) =>
    """
    Queue a sequence of sequences of bytes on linux.
    Do nothing on windows.
    """
    get_handler().queuev(data)

  be send_queue() =>
    """
    Write pending queue to network on linux.
    Do nothing on windows.
    """
    get_handler().send_queue()

  be mute(d: Any tag) =>
    """
    Temporarily suspend reading off this TCPConnection until such time as
    `unmute` is called.
    """
    get_handler().mute(d)

  be unmute(d: Any tag) =>
    """
    Start reading off this TCPConnection again after having been muted.
    """
    get_handler().unmute(d)

  be set_notify(notify: CustomTCPConnectionNotify iso) =>
    """
    Change the notifier.
    """
    get_handler().set_notify(consume notify)

  be dispose() =>
    """
    Close the connection gracefully once all writes are sent.
    """
    get_handler().dispose()

  be _event_notify(event: AsioEventID, flags: U32, arg: U32) =>
    """
    Handle socket events.
    """
    get_handler().event_notify(event, flags, arg)

  be _read_again() =>
    """
    Resume reading.
    """
    get_handler().pending_reads()

  fun ref expect(qty: USize = 0) =>
    """
    A `received` call on the notifier must contain exactly `qty` bytes. If
    `qty` is zero, the call can contain any amount of data. This has no effect
    if called in the `sent` notifier callback.
    """
    get_handler().expect(qty)

  be reconnect() =>
    get_handler().reconnect()

