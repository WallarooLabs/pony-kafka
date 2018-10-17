/*

Copyright (C) 2016-2017, Sendence LLC
Copyright (C) 2016-2017, The Pony Developers
Copyright (c) 2014-2015, Causality Ltd.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND
ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE LIABLE FOR
ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
(INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

*/

use "collections"
use "net"

use @pony_asio_event_create[AsioEventID](owner: AsioEventNotify, fd: U32,
  flags: U32, nsec: U64, noisy: Bool)
use @pony_asio_event_fd[U32](event: AsioEventID)
use @pony_asio_event_unsubscribe[None](event: AsioEventID)
use @pony_asio_event_resubscribe_read[None](event: AsioEventID)
use @pony_asio_event_resubscribe_write[None](event: AsioEventID)
use @pony_asio_event_destroy[None](event: AsioEventID)
use @pony_asio_event_set_writeable[None](event: AsioEventID, writeable: Bool)
use @pony_asio_event_set_readable[None](event: AsioEventID, readable: Bool)

// TODO: Move some of the logic into primitives that take a handler ref to work
// on to hopefully allow for reusability between tcp/udp/etc connections
class CustomTCPConnectionHandler is TCPConnectionHandler
  var _listen: (TCPListener | None) = None
  var notify: CustomTCPConnectionNotify
  var _connect_count: U32
  var _fd: U32 = -1
  var _event: AsioEventID = AsioEvent.none()
  var _connected: Bool = false
  var _readable: Bool = false
  var _reading: Bool = false
  var _writeable: Bool = false
  var _throttled: Bool = false
  var _closed: Bool = false
  var _shutdown: Bool = false
  var _shutdown_peer: Bool = false
  var _in_sent: Bool = false

  embed _pending_writev_posix: Array[(Pointer[U8] tag, USize)] = _pending_writev_posix.create()
  embed _pending_writev_windows: Array[(USize, Pointer[U8] tag)] = _pending_writev_windows.create()

  var _pending_sent: USize = 0
  var _pending_writev_total: USize = 0

  var _read_buf: Array[U8] iso
  var _read_buf_offset: USize = 0
  var _max_received_count: USize = 50

  var _next_size: USize
  let _max_size: USize

  var _expect: USize = 0

  var _muted: Bool = false
  let _muted_by: SetIs[Any tag] = _muted_by.create()

  let _conn: CustomTCPConnection

  let _host: String
  let _service: String
  let _from: String

  new create(
    conn: CustomTCPConnection,
    auth: TCPConnectionAuth,
    notify': CustomTCPConnectionNotify iso,
    host: String,
    service: String,
    from: String = "",
    init_size: USize = 64,
    max_size: USize = 16384)
  =>
    """
    Connect via IPv4 or IPv6. If `from` is a non-empty string, the connection
    will be made from the specified interface.
    """
    _conn = conn
    _host = host
    _service = service
    _from = from
    _read_buf = recover Array[U8] .> undefined(init_size) end
    _next_size = init_size
    _max_size = max_size
    notify = consume notify'
    _connect_count =
      @pony_os_connect_tcp[U32](conn, host.cstring(), service.cstring(),
        from.cstring())
    _notify_connecting()

  new ip4(
    conn: CustomTCPConnection,
    auth: TCPConnectionAuth,
    notify': CustomTCPConnectionNotify iso,
    host: String,
    service: String,
    from: String = "",
    init_size: USize = 64,
    max_size: USize = 16384)
  =>
    """
    Connect via IPv4.
    """
    _conn = conn
    _host = host
    _service = service
    _from = from
    _read_buf = recover Array[U8] .> undefined(init_size) end
    _next_size = init_size
    _max_size = max_size
    notify = consume notify'
    _connect_count =
      @pony_os_connect_tcp4[U32](conn, host.cstring(), service.cstring(),
        from.cstring())
    _notify_connecting()

  new ip6(
    conn: CustomTCPConnection,
    auth: TCPConnectionAuth,
    notify': CustomTCPConnectionNotify iso,
    host: String,
    service: String,
    from: String = "",
    init_size: USize = 64,
    max_size: USize = 16384)
  =>
    """
    Connect via IPv6.
    """
    _conn = conn
    _host = host
    _service = service
    _from = from
    _read_buf = recover Array[U8] .> undefined(init_size) end
    _next_size = init_size
    _max_size = max_size
    notify = consume notify'
    _connect_count =
      @pony_os_connect_tcp6[U32](conn, host.cstring(), service.cstring(),
        from.cstring())
    _notify_connecting()

  new accept(
    conn: CustomTCPConnection,
    listen: TCPListener,
    notify': CustomTCPConnectionNotify iso,
    fd: U32,
    init_size: USize = 64,
    max_size: USize = 16384)
  =>
    """
    A new connection accepted on a server.
    """
    _conn = conn

    // TODO get correct values
    _host = "" // host
    _service = "" // service
    _from = "" // from
    _listen = listen
    notify = consume notify'
    _connect_count = 0
    _fd = fd
    ifdef not windows then
      _event = @pony_asio_event_create(conn, fd,
        AsioEvent.read_write_oneshot(), 0, true)
    else
      _event = @pony_asio_event_create(conn, fd,
        AsioEvent.read_write(), 0, true)
    end
    _connected = true
    ifdef not windows then
      @pony_asio_event_set_writeable(_event, true)
    end
    _writeable = true
    _read_buf = recover Array[U8] .> undefined(init_size) end
    _next_size = init_size
    _max_size = max_size

    notify.accepted(conn)

    _readable = true
    _queue_read()
    pending_reads()

  fun ref write(data: ByteSeq) =>
    """
    Write a single sequence of bytes. Data will be silently discarded if the
    connection has not yet been established though.
    """
    if _connected and not _closed then
      _in_sent = true
      write_final(notify.sent(_conn, data))
      _in_sent = false
    end

  fun ref queue(data: ByteSeq) =>
    """
    Queue a single sequence of bytes on linux.
    Compile error on windows.
    """
    ifdef not windows then
      _pending_writev_posix .> push((data.cpointer(), data.size()))
      _pending_writev_total = _pending_writev_total + data.size()
    else
      compile_error "no queueing on windows"
    end

  fun ref writev(data: ByteSeqIter) =>
    """
    Write a sequence of sequences of bytes. Data will be silently discarded if
    the connection has not yet been established though.
    """
    if _connected and not _closed then
      _in_sent = true

      ifdef windows then
        try
          var num_to_send: I32 = 0
          for bytes in notify.sentv(_conn, data).values() do
            // Add an IOCP write.
            _pending_writev_windows
              .> push((bytes.size(), bytes.cpointer()))
            _pending_writev_total = _pending_writev_total + bytes.size()
            num_to_send = num_to_send + 1
          end

          // Write as much data as possible.
          var len =
            @pony_os_writev[USize](_event,
              _pending_writev_windows.cpointer(_pending_sent),
              num_to_send) ?

          _pending_sent = _pending_sent + num_to_send.usize()

          if _pending_sent > 32 then
            // If more than 32 asynchronous writes are scheduled, apply
            // backpressure. The choice of 32 is rather arbitrary an
            // probably needs tuning
            _apply_backpressure()
          end
        end
      else
        for bytes in notify.sentv(_conn, data).values() do
          _pending_writev_posix
            .> push((bytes.cpointer(), bytes.size()))
          _pending_writev_total = _pending_writev_total + bytes.size()
        end

        pending_writes()
      end

      _in_sent = false
    end

  fun ref queuev(data: ByteSeqIter) =>
    """
    Queue a sequence of sequences of bytes on linux.
    Compile error on windows.
    """

    ifdef not windows then
      for bytes in notify.sentv(_conn, data).values() do
        _pending_writev_posix .> push((bytes.cpointer(), bytes.size()))
        _pending_writev_total = _pending_writev_total + bytes.size()
      end
    else
      compile_error "no queueing on windows"
    end

  fun ref send_queue() =>
    """
    Write pending queue to network on linux.
    Compile error on windows.
    """
    ifdef not windows then
      pending_writes()
    else
      compile_error "no queueing on windows"
    end

  fun ref mute(d: Any tag) =>
    """
    Temporarily suspend reading off this TCPConnection until such time as
    `unmute` is called.
    """
    _muted_by.set(d)
    _muted = true

  fun ref unmute(d: Any tag) =>
    """
    Start reading off this TCPConnection again after having been muted.
    """
    _muted_by.unset(d)

    if _muted_by.size() == 0 then
      _muted = false
      if not _reading then
        pending_reads()
      end
    end

  fun ref set_notify(notify': CustomTCPConnectionNotify iso) =>
    """
    Change the notifier.
    """
    notify = consume notify'

  fun ref dispose() =>
    """
    Close the connection gracefully once all writes are sent.
    """
    close()
    notify.dispose()

  fun local_address(): NetAddress =>
    """
    Return the local IP address.
    """
    let ip = recover NetAddress end
    @pony_os_sockname[Bool](_fd, ip)
    ip

  fun remote_address(): NetAddress =>
    """
    Return the remote IP address.
    """
    let ip = recover NetAddress end
    @pony_os_peername[Bool](_fd, ip)
    ip

  fun requested_address(): (String, String) =>
    """
    Return the host and service that were originally provided to the
    @pony_os_listen_tcp method.
    """
    (_host, _service)

  fun ref expect(qty: USize = 0) =>
    """
    A `received` call on the notifier must contain exactly `qty` bytes. If
    `qty` is zero, the call can contain any amount of data. This has no effect
    if called in the `sent` notifier callback.
    """
    if not _in_sent then
      _expect = notify.expect(_conn, qty)
      _read_buf_size()
    end

  fun ref set_nodelay(state: Bool) =>
    """
    Turn Nagle on/off. Defaults to on. This can only be set on a connected
    socket.
    """
    if _connected then
      set_tcp_nodelay(state)
    end

  fun ref set_keepalive(secs: U32) =>
    """
    Sets the TCP keepalive timeout to approximately `secs` seconds. Exact
    timing is OS dependent. If `secs` is zero, TCP keepalive is disabled. TCP
    keepalive is disabled by default. This can only be set on a connected
    socket.
    """
    if _connected then
      @pony_os_keepalive[None](_fd, secs)
    end

  fun ref event_notify(event: AsioEventID, flags: U32, arg: U32) =>
    """
    Handle socket events.
    """
    if event isnt _event then
      if AsioEvent.writeable(flags) then
        // A connection has completed.
        var fd = @pony_asio_event_fd(event)
        _connect_count = _connect_count - 1

        if (not _connected and not _closed)
          or (not _connected and _closed) then
          // We don't have a connection yet
          // or we have a re-connection after a disonnect.

          if _is_sock_connected(fd) then
            // The connection was successful, make it ours.
            _fd = fd
            _event = event

            // clear anything pending to be sent because on recovery we're
            // going to have to replay from our queue when requested
            // for new connections this is effectively a no-op
            ifdef not windows then
              _pending_writev_posix.clear()
            else
              _pending_writev_windows.clear()
            end
            _pending_writev_total = 0

            _connected = true
            _writeable = true
            _readable = true

            _closed = false
            _shutdown = false
            _shutdown_peer = false

            notify.connected(_conn)
            _queue_read()
            pending_reads()

            // Don't call _complete_writes, as Windows will see this as a
            // closed connection.
            ifdef not windows then
              if pending_writes() then
                // Sent all data; release backpressure
                _release_backpressure()
              end
            end

          else
            // The connection failed, unsubscribe the event and close.
            @pony_asio_event_unsubscribe(event)
            @pony_os_socket_close[None](fd)
            _notify_connecting()
          end
        else
          // We're already connected, unsubscribe the event and close.
          @pony_asio_event_unsubscribe(event)
          @pony_os_socket_close[None](fd)
          _try_shutdown()
        end
      else
        // It's not our event.
        if AsioEvent.disposable(flags) then
          // It's disposable, so dispose of it.
          @pony_asio_event_destroy(event)
        end
      end
    else
      // At this point, it's our event.
      if _connected and not _shutdown_peer then
        if AsioEvent.writeable(flags) then
          _writeable = true
          _complete_writes(arg)
            ifdef not windows then
              if pending_writes() then
                // Sent all data; release backpressure
                _release_backpressure()
              end
            end
        end

        if AsioEvent.readable(flags) then
          _readable = true
          _complete_reads(arg)
          pending_reads()
        end
      end

      if AsioEvent.disposable(flags) then
        @pony_asio_event_destroy(event)
        _event = AsioEvent.none()
      end

      _try_shutdown()
    end

  fun ref write_final(data: ByteSeq) =>
    """
    Write as much as possible to the socket. Set `_writeable` to `false` if not
    everything was written. On an error, close the connection. This is for data
    that has already been transformed by the notifier. Data will be silently
    discarded if the connection has not yet been established though.
    """
    if _connected and not _closed then
      ifdef windows then
        try
          // Add an IOCP write.
          _pending_writev_windows .> push((data.size(), data.cpointer()))
          _pending_writev_total = _pending_writev_total + data.size()

          @pony_os_writev[USize](_event,
            _pending_writev_windows.cpointer(_pending_sent), I32(1)) ?

          _pending_sent = _pending_sent + 1

          if _pending_sent > 32 then
            // If more than 32 asynchronous writes are scheduled, apply
            // backpressure. The choice of 32 is rather arbitrary an
            // probably needs tuning
            _apply_backpressure()
          end
        end
      else
        _pending_writev_posix .> push((data.cpointer(), data.size()))
        _pending_writev_total = _pending_writev_total + data.size()
        pending_writes()
      end
    end

  fun ref _complete_writes(len: U32) =>
    """
    The OS has informed us that `len` bytes of pending writes have completed.
    This occurs only with IOCP on Windows.
    """
    ifdef windows then
      if len == 0 then
        // IOCP reported a failed write on this chunk. Non-graceful shutdown.
        _hard_close()
        return
      end

      try
        _manage_pending_buffer(len.usize(),
          _pending_writev_total, _pending_writev_windows.size())?
      end

      if _pending_sent < 16 then
        // If fewer than 16 asynchronous writes are scheduled, remove
        // backpressure. The choice of 16 is rather arbitrary and probably
        // needs to be tuned.
        _release_backpressure()
      end
    end

  fun ref pending_writes(): Bool =>
    """
    Send pending data. If any data can't be sent, keep it and mark as not
    writeable. On an error, dispose of the connection. Returns whether
    it sent all pending data or not.
    """
    ifdef not windows then
      // TODO: Make writev_batch_size user configurable
      let writev_batch_size: USize = @pony_os_writev_max[I32]().usize()
      var num_to_send: USize = 0
      var bytes_to_send: USize = 0
      var bytes_sent: USize = 0
      while _writeable and not _shutdown_peer and (_pending_writev_total > 0) do
        if bytes_sent > _max_size then
          _conn._write_again()
          return false
        end
        try
          // Determine number of bytes and buffers to send.
          if _pending_writev_posix.size() < writev_batch_size then
            num_to_send = _pending_writev_posix.size()
            bytes_to_send = _pending_writev_total
          else
            // Have more buffers than a single writev can handle.
            // Iterate over buffers being sent to add up total.
            num_to_send = writev_batch_size
            bytes_to_send = 0
            for d in Range[USize](0, num_to_send, 1) do
              bytes_to_send = bytes_to_send + _pending_writev_posix(d)?._2
            end
          end

          // Write as much data as possible.
          var len = @pony_os_writev[USize](_event,
            _pending_writev_posix.cpointer(), num_to_send.i32()) ?

          if _manage_pending_buffer(len, bytes_to_send, num_to_send)? then
            return true
          end

          bytes_sent = bytes_sent + len
        else
          // Non-graceful shutdown on error.
          _hard_close()
        end
      end
    end

    false

  fun ref _manage_pending_buffer(
    bytes_sent: USize,
    bytes_to_send: USize,
    num_to_send: USize)
    : Bool ?
  =>
    """
    Manage pending buffer for data sent. Returns a boolean of whether
    the pending buffer is empty or not.
    """
    var len = bytes_sent
    if len < bytes_to_send then
      var num_sent: USize = 0
      while len > 0 do
        (let iov_p, let iov_s) =
          ifdef windows then
            (let tmp_s, let tmp_p) = _pending_writev_windows(0)?
            (tmp_p, tmp_s)
          else
            _pending_writev_posix(0)?
          end
        if iov_s <= len then
          num_sent = num_sent + 1
          len = len - iov_s
          _pending_writev_total = _pending_writev_total - iov_s
        else
          ifdef windows then
            _pending_writev_windows(0)? = (iov_s-len, iov_p.offset(len))
          else
            _pending_writev_posix(0)? = (iov_p.offset(len), iov_s-len)
          end
          _pending_writev_total = _pending_writev_total - len
          len = 0
        end
      end

      ifdef windows then
        // do a trim in place instead of many shifts for efficiency
        _pending_writev_windows.trim_in_place(num_sent)
        _pending_sent = _pending_sent - num_sent
      else
        // do a trim in place instead of many shifts for efficiency
        _pending_writev_posix.trim_in_place(num_sent)
      end

      ifdef not windows then
        _apply_backpressure()
      end
    else
      // sent all data we requested in this batch
      _pending_writev_total = _pending_writev_total - bytes_to_send
      if _pending_writev_total == 0 then
        ifdef windows then
          // do a trim in place instead of a clear to free up memory
          _pending_writev_windows.trim_in_place(_pending_writev_windows.size())
          _pending_sent = 0
        else
          // do a trim in place instead of a clear to free up memory
          _pending_writev_posix.trim_in_place(_pending_writev_posix.size())
        end
        return true
      else
        ifdef windows then
          // do a trim in place instead of many shifts for efficiency
          _pending_writev_windows.trim_in_place(num_to_send)
          _pending_sent = _pending_sent - 1
        else
          // do a trim in place instead of many shifts for efficiency
          _pending_writev_posix.trim_in_place(num_to_send)
        end
      end
    end

    false

  fun ref _complete_reads(len: U32) =>
    """
    The OS has informed us that `len` bytes of pending reads have completed.
    This occurs only with IOCP on Windows.
    """
    ifdef windows then
      match len.usize()
      | 0 =>
        // The socket has been closed from the other side, or a hard close has
        // cancelled the queued read.
        _readable = false
        _shutdown_peer = true
        close()
        return
      | _next_size =>
        _next_size = _max_size.min(_next_size * 2)
      end

      _read_buf_offset = _read_buf_offset + len.usize()

      if (not _muted) and (_read_buf_offset >= _expect) then
        let data = _read_buf = recover Array[U8] end
        data.truncate(_read_buf_offset)
        _read_buf_offset = 0

        notify.received(_conn, consume data, 1)
        _read_buf_size()
      end

      _queue_read()
    end

  fun ref _read_buf_size() =>
    """
    Resize the read buffer.
    """
    if _expect != 0 then
      _read_buf.undefined(_expect.next_pow2().max(_next_size))
    else
      _read_buf.undefined(_next_size)
    end

  fun ref _queue_read() =>
    """
    Begin an IOCP read on Windows.
    """
    ifdef windows then
      try
        @pony_os_recv[USize](
          _event,
          _read_buf.cpointer(_read_buf_offset),
          _read_buf.size() - _read_buf_offset) ?
      else
        _hard_close()
      end
    end

  fun ref pending_reads() =>
    """
    Unless this connection is currently muted, read while data is available,
    guessing the next packet length as we go. If we read 5 kb of data, send
    ourself a resume message and stop reading, to avoid starving other actors.
    Currently we can handle a varying value of _expect (greater than 0) and
    constant _expect of 0 but we cannot handle switching between these two
    cases.
    """
    ifdef not windows then
      try
        var sum: USize = 0
        var received_count: USize = 0
        _reading = true

        while _readable and not _shutdown_peer do
          // exit if muted
          if _muted then
            _reading = false
            return
          end

          // distribute the data we've already read that is in the `read_buf`
          // and able to be distributed
          while (_read_buf_offset >= _expect) and (_read_buf_offset > 0) do
            // get data to be distributed and update `_read_buf_offset`
            let data =
              if _expect == 0 then
                let data' = _read_buf = recover Array[U8] end
                data'.truncate(_read_buf_offset)
                _read_buf_offset = 0
                consume data'
              else
                let x = _read_buf = recover Array[U8] end
                (let data', _read_buf) = (consume x).chop(_expect)
                _read_buf_offset = _read_buf_offset - _expect
                consume data'
              end

            // increment max reads
            received_count = received_count + 1

            // check if we should yield to let another actor run
            if (not notify.received(_conn, consume data,
              received_count))
              or (received_count >= _max_received_count)
            then
              _read_buf_size()
              _conn._read_again()
              _reading = false
              return
            end
          end

          if sum >= _max_size then
            // If we've read _max_size, yield and read again later.
            _read_buf_size()
            _conn._read_again()
            _reading = false
            return
          end

          // make sure we have enough space to read enough data for _expect
          if _read_buf.size() < _expect then
            _read_buf_size()
          end

          // Read as much data as possible.
          let len = @pony_os_recv[USize](
            _event,
            _read_buf.cpointer(_read_buf_offset),
            _read_buf.size() - _read_buf_offset) ?

          match len
          | 0 =>
            // Would block, try again later.
            // this is safe because asio thread isn't currently subscribed
            // for a read event so will not be writing to the readable flag
            @pony_asio_event_set_readable[None](_event, false)
            _readable = false
            _reading = false
            @pony_asio_event_resubscribe_read(_event)
            return
          | (_read_buf.size() - _read_buf_offset) =>
            // Increase the read buffer size.
            _next_size = _max_size.min(_next_size * 2)
          end

          _read_buf_offset = _read_buf_offset + len
          sum = sum + len
        end
      else
        // The socket has been closed from the other side.
        _shutdown_peer = true
        _hard_close()
      end
    end

    _reading = false

  fun ref _notify_connecting() =>
    """
    Inform the notifier that we're connecting.
    """
    if _connect_count > 0 then
      notify.connecting(_conn, _connect_count)
    else
      notify.connect_failed(_conn)
      _hard_close()
    end

  fun ref _notify_reconnecting() =>
    """
    Inform the notifier that we're reconnecting.
    """
    notify.before_reconnecting(_conn)
    _notify_connecting()

  fun ref close() =>
    """
    Attempt to perform a graceful shutdown. Don't accept new writes. If the
    connection isn't muted then we won't finish closing until we get a zero
    length read. If the connection is muted, perform a hard close and shut
    down immediately.
    """
    ifdef windows then
      _close()
    else
      if _muted then
        _hard_close()
      else
        _close()
      end
    end

  fun ref _close() =>
    _closed = true
    _try_shutdown()

  fun ref _try_shutdown() =>
    """
    If we have closed and we have no remaining writes or pending connections,
    then shutdown.
    """
    if not _closed then
      return
    end

    if
      not _shutdown and
      (_connect_count == 0) and
      (_pending_writev_total == 0)
    then
      _shutdown = true

      if _connected then
        @pony_os_socket_shutdown[None](_fd)
      else
        _shutdown_peer = true
      end
    end

    if _connected and _shutdown and _shutdown_peer then
      _hard_close()
    end

    ifdef windows then
      // On windows, wait until all outstanding IOCP operations have completed
      // or been cancelled.
      if not _connected and not _readable and (_pending_sent == 0) then
        @pony_asio_event_unsubscribe(_event)
      end
    end

  fun ref _hard_close() =>
    """
    When an error happens, do a non-graceful close.
    """
    if not _connected then
      return
    end

    _connected = false
    _closed = true
    _shutdown = true
    _shutdown_peer = true

    _pending_writev_total = 0
    ifdef windows then
      _pending_writev_windows.clear()
      _pending_sent = 0
    else
      _pending_writev_posix.clear()
    end

    ifdef not windows then
      // Unsubscribe immediately and drop all pending writes.
      @pony_asio_event_unsubscribe(_event)
      _readable = false
      _writeable = false
      @pony_asio_event_set_readable(_event, false)
      @pony_asio_event_set_writeable(_event, false)
    end

    // On windows, this will also cancel all outstanding IOCP operations.
    @pony_os_socket_close[None](_fd)
    _fd = -1

    notify.closed(_conn)

// TODO: Uncomment when this is a full replacement for net package and includes
// the listener stuff
//    try (_listen as TCPListener)._conn_closed() end


  // Check this when a connection gets its first writeable event.
  fun _is_sock_connected(fd: U32): Bool =>
    (let errno: U32, let value: U32) = _OSSocket.get_so_error(fd)
    (errno == 0) and (value == 0)

  fun ref _apply_backpressure() =>
    if not _throttled then
      _throttled = true
      notify.throttled(_conn)
    end

    ifdef not windows then
      _writeable = false

      // this is safe because asio thread isn't currently subscribed
      // for a write event so will not be writing to the readable flag
      @pony_asio_event_set_writeable[None](_event, false)
      @pony_asio_event_resubscribe_write(_event)
    end

  fun ref _release_backpressure() =>
    if _throttled then
      _throttled = false
      notify.unthrottled(_conn)
    end

  fun ref reconnect() =>
    if not _connected then
      _connect_count = @pony_os_connect_tcp[U32](_conn,
        _host.cstring(), _service.cstring(),
        _from.cstring())
      _notify_reconnecting()
    end

  /**************************************/

  fun ref getsockopt(level: I32, option_name: I32, option_max_size: USize = 4):
    (U32, Array[U8] iso^) =>
    """
    General wrapper for TCP sockets to the `getsockopt(2)` system call.

    The caller must provide an array that is pre-allocated to be
    at least as large as the largest data structure that the kernel
    may return for the requested option.

    In case of system call success, this function returns the 2-tuple:
    1. The integer `0`.
    2. An `Array[U8]` of data returned by the system call's `void *`
       4th argument.  Its size is specified by the kernel via the
       system call's `sockopt_len_t *` 5th argument.

    In case of system call failure, this function returns the 2-tuple:
    1. The value of `errno`.
    2. An undefined value that must be ignored.

    Usage example:

    ```pony
    // connected() is a callback function for class TCPConnectionNotify
    fun ref connected(conn: TCPConnection ref) =>
      match conn.getsockopt(OSSockOpt.sol_socket(), OSSockOpt.so_rcvbuf(), 4)
        | (0, let gbytes: Array[U8] iso) =>
          try
            let br = Reader.create().>append(consume gbytes)
            ifdef littleendian then
              let buffer_size = br.u32_le()?
            else
              let buffer_size = br.u32_be()?
            end
          end
        | (let errno: U32, _) =>
          // System call failed
      end
    ```
    """
    _OSSocket.getsockopt(_fd, level, option_name, option_max_size)

  fun ref getsockopt_u32(level: I32, option_name: I32): (U32, U32) =>
    """
    Wrapper for TCP sockets to the `getsockopt(2)` system call where
    the kernel's returned option value is a C `uint32_t` type / Pony
    type `U32`.

    In case of system call success, this function returns the 2-tuple:
    1. The integer `0`.
    2. The `*option_value` returned by the kernel converted to a Pony `U32`.

    In case of system call failure, this function returns the 2-tuple:
    1. The value of `errno`.
    2. An undefined value that must be ignored.
    """
    _OSSocket.getsockopt_u32(_fd, level, option_name)

  fun ref setsockopt(level: I32, option_name: I32, option: Array[U8]): U32 =>
    """
    General wrapper for TCP sockets to the `setsockopt(2)` system call.

    The caller is responsible for the correct size and byte contents of
    the `option` array for the requested `level` and `option_name`,
    including using the appropriate machine endian byte order.

    This function returns `0` on success, else the value of `errno` on
    failure.

    Usage example:

    ```pony
    // connected() is a callback function for class TCPConnectionNotify
    fun ref connected(conn: TCPConnection ref) =>
      let sb = Writer

      sb.u32_le(7744)             // Our desired socket buffer size
      let sbytes = Array[U8]
      for bs in sb.done().values() do
        sbytes.append(bs)
      end
      match conn.setsockopt(OSSockOpt.sol_socket(), OSSockOpt.so_rcvbuf(), sbytes)
        | 0 =>
          // System call was successful
        | let errno: U32 =>
          // System call failed
      end
    ```
    """
    _OSSocket.setsockopt(_fd, level, option_name, option)

  fun ref setsockopt_u32(level: I32, option_name: I32, option: U32): U32 =>
    """
    General wrapper for TCP sockets to the `setsockopt(2)` system call where
    the kernel expects an option value of a C `uint32_t` type / Pony
    type `U32`.

    This function returns `0` on success, else the value of `errno` on
    failure.
    """
    _OSSocket.setsockopt_u32(_fd, level, option_name, option)


  fun ref get_so_error(): (U32, U32) =>
    """
    Wrapper for the FFI call `getsockopt(fd, SOL_SOCKET, SO_ERROR, ...)`
    """
    _OSSocket.get_so_error(_fd)

  fun ref get_so_rcvbuf(): (U32, U32) =>
    """
    Wrapper for the FFI call `getsockopt(fd, SOL_SOCKET, SO_RCVBUF, ...)`
    """
    _OSSocket.get_so_rcvbuf(_fd)

  fun ref get_so_sndbuf(): (U32, U32) =>
    """
    Wrapper for the FFI call `getsockopt(fd, SOL_SOCKET, SO_SNDBUF, ...)`
    """
    _OSSocket.get_so_sndbuf(_fd)

  fun ref get_tcp_nodelay(): (U32, U32) =>
    """
    Wrapper for the FFI call `getsockopt(fd, SOL_SOCKET, TCP_NODELAY, ...)`
    """
    _OSSocket.getsockopt_u32(_fd, OSSockOpt.sol_socket(), OSSockOpt.tcp_nodelay())


  fun ref set_so_rcvbuf(bufsize: U32): U32 =>
    """
    Wrapper for the FFI call `setsockopt(fd, SOL_SOCKET, SO_RCVBUF, ...)`
    """
    _OSSocket.set_so_rcvbuf(_fd, bufsize)

  fun ref set_so_sndbuf(bufsize: U32): U32 =>
    """
    Wrapper for the FFI call `setsockopt(fd, SOL_SOCKET, SO_SNDBUF, ...)`
    """
    _OSSocket.set_so_sndbuf(_fd, bufsize)

  fun ref set_tcp_nodelay(state: Bool): U32 =>
    """
    Wrapper for the FFI call `setsockopt(fd, SOL_SOCKET, TCP_NODELAY, ...)`
    """
    var word: Array[U8] ref =
      _OSSocket.u32_to_bytes4(if state then 1 else 0 end)
    _OSSocket.setsockopt(_fd, OSSockOpt.sol_socket(), OSSockOpt.tcp_nodelay(), word)
