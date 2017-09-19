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
  flags: U32, nsec: U64, noisy: Bool, auto_resub: Bool)
use @pony_asio_event_fd[U32](event: AsioEventID)
use @pony_asio_event_unsubscribe[None](event: AsioEventID)
use @pony_asio_event_resubscribe_read[None](event: AsioEventID)
use @pony_asio_event_resubscribe_write[None](event: AsioEventID)
use @pony_asio_event_destroy[None](event: AsioEventID)

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
  var _writeable: Bool = false
  var _closed: Bool = false
  var _shutdown: Bool = false
  var _shutdown_peer: Bool = false
  var _in_sent: Bool = false

  embed _pending: List[(ByteSeq, USize)] = _pending.create()
  embed _pending_writev: Array[USize] = _pending_writev.create()
  var _pending_writev_total: USize = 0

  var _read_buf: Array[U8] iso

  var _next_size: USize
  let _max_size: USize

  var _read_len: USize = 0
  var _expect: USize = 0

  var _muted: Bool = false
  let _muted_downstream: SetIs[Any tag] = _muted_downstream.create()

  let _conn: CustomTCPConnection

  let _host: String
  let _service: String
  let _from: String

  new create(conn: CustomTCPConnection, auth: TCPConnectionAuth,
    notify': CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
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
    _read_buf = recover Array[U8].>undefined(init_size) end
    _next_size = init_size
    _max_size = max_size
    notify = consume notify'
    _connect_count = @pony_os_connect_tcp[U32](conn,
      host.cstring(), service.cstring(),
      from.cstring())
    _notify_connecting()

  new ip4(conn: CustomTCPConnection, auth: TCPConnectionAuth,
    notify': CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
    max_size: USize = 16384)
  =>
    """
    Connect via IPv4.
    """
    _conn = conn
    _host = host
    _service = service
    _from = from
    _read_buf = recover Array[U8].>undefined(init_size) end
    _next_size = init_size
    _max_size = max_size
    notify = consume notify'
    _connect_count = @pony_os_connect_tcp4[U32](conn,
      host.cstring(), service.cstring(),
      from.cstring())
    _notify_connecting()

  new ip6(conn: CustomTCPConnection, auth: TCPConnectionAuth,
    notify': CustomTCPConnectionNotify iso,
    host: String, service: String, from: String = "", init_size: USize = 64,
    max_size: USize = 16384)
  =>
    """
    Connect via IPv6.
    """
    _conn = conn
    _host = host
    _service = service
    _from = from
    _read_buf = recover Array[U8].>undefined(init_size) end
    _next_size = init_size
    _max_size = max_size
    notify = consume notify'
    _connect_count = @pony_os_connect_tcp6[U32](conn,
      host.cstring(), service.cstring(),
      from.cstring())
    _notify_connecting()

  new accept(conn: CustomTCPConnection, listen: TCPListener,
    notify': CustomTCPConnectionNotify iso, fd: U32,
    init_size: USize = 64, max_size: USize = 16384)
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
    ifdef linux then
      _event = @pony_asio_event_create(conn, fd,
        AsioEvent.read_write_oneshot(), 0, true, true)
    else
      _event = @pony_asio_event_create(conn, fd,
        AsioEvent.read_write(), 0, true, false)
    end
    _connected = true
    ifdef linux then
      @pony_asio_event_set_writeable[None](_event, true)
    end
    _writeable = true
    _read_buf = recover Array[U8].>undefined(init_size) end
    _next_size = init_size
    _max_size = max_size

    notify.accepted(conn)
    _queue_read()

  fun ref write(data: ByteSeq) =>
    """
    Write a single sequence of bytes.
    """
    if not _closed then
      _in_sent = true
      write_final(notify.sent(_conn, data))
      _in_sent = false
    end

  fun ref queue(data: ByteSeq) =>
    """
    Queue a single sequence of bytes on linux.
    Do nothing on windows.
    """
    ifdef not windows then
      _pending_writev.>push(data.cpointer().usize()).>push(data.size())
      _pending_writev_total = _pending_writev_total + data.size()
      _pending.push((data, 0))
    end

  fun ref writev(data: ByteSeqIter) =>
    """
    Write a sequence of sequences of bytes.
    """

    if not _closed then
      _in_sent = true

      ifdef windows then
        for bytes in notify.sentv(_conn, data).values() do
          write_final(bytes)
        end
      else
        for bytes in notify.sentv(_conn, data).values() do
          _pending_writev.>push(bytes.cpointer().usize()).>push(bytes.size())
          _pending_writev_total = _pending_writev_total + bytes.size()
          _pending.push((bytes, 0))
        end

        _pending_writes()
      end

      _in_sent = false
    end

  fun ref queuev(data: ByteSeqIter) =>
    """
    Queue a sequence of sequences of bytes on linux.
    Do nothing on windows.
    """

    ifdef not windows then
      for bytes in notify.sentv(_conn, data).values() do
        _pending_writev.>push(bytes.cpointer().usize()).>push(bytes.size())
        _pending_writev_total = _pending_writev_total + bytes.size()
        _pending.push((bytes, 0))
      end
    end

  fun ref send_queue() =>
    """
    Write pending queue to network on linux.
    Do nothing on windows.
    """
    ifdef not windows then
      _pending_writes()
    end

  fun ref mute(d: Any tag) =>
    """
    Temporarily suspend reading off this TCPConnection until such time as
    `unmute` is called.
    """
    _muted_downstream.set(d)
    _muted = true

  fun ref unmute(d: Any tag) =>
    """
    Start reading off this TCPConnection again after having been muted.
    """
    _muted_downstream.unset(d)

    if _muted_downstream.size() == 0 then
      _muted = false
      pending_reads()
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
      @pony_os_nodelay[None](_fd, state)
    end

  fun ref set_keepalive(secs: U32) =>
    """
    Sets the TCP keepalive timeout to approximately secs seconds. Exact timing
    is OS dependent. If secs is zero, TCP keepalive is disabled. TCP keepalive
    is disabled by default. This can only be set on a connected socket.
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

        if not _connected and not _closed then
          // We don't have a connection yet.
          if @pony_os_connected[Bool](fd) then
            // The connection was successful, make it ours.
            _fd = fd
            _event = event
            _connected = true
            _writeable = true

            notify.connected(_conn)
            _queue_read()

            // Don't call _complete_writes, as Windows will see this as a
            // closed connection.
            ifdef not windows then
              if _pending_writes() then
                //sent all data; release backpressure
                _release_backpressure()
              end
            end
          else
            // The connection failed, unsubscribe the event and close.
            @pony_asio_event_unsubscribe(event)
            @pony_os_socket_close[None](fd)
            _notify_connecting()
          end
        elseif not _connected and _closed then
          @printf[I32]("Reconnection asio event\n".cstring())

          if @pony_os_connected[Bool](fd) then
            // The connection was successful, make it ours.
            _fd = fd
            _event = event

            // clear anything pending to be sent because on recovery we're
            // going to have to replay from our queue when requested
            _pending_writev.clear()
            _pending.clear()
            _pending_writev_total = 0

            _connected = true
            _writeable = true

            _closed = false
            _shutdown = false
            _shutdown_peer = false

            notify.connected(_conn)
            _queue_read()

            // Don't call _complete_writes, as Windows will see this as a
            // closed connection.
            ifdef not windows then
              if _pending_writes() then
                //sent all data; release backpressure
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
              if _pending_writes() then
                //sent all data; release backpressure
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
    Write as much as possible to the socket. Set _writeable to false if not
    everything was written. On an error, close the connection. This is for
    data that has already been transformed by the notifier.
    """
    if not _closed then
      ifdef windows then
        try
          // Add an IOCP write.
          @pony_os_send[USize](_event, data.cpointer(), data.size()) ?
          _pending.push((data, 0))

          if _pending.size() > 32 then
            // If more than 32 asynchronous writes are scheduled, apply
            // backpressure. The choice of 32 is rather arbitrary an
            // probably needs tuning
            _apply_backpressure()
          end
        end
      else
        _pending_writev.>push(data.cpointer().usize()).>push(data.size())
        _pending_writev_total = _pending_writev_total + data.size()
        _pending.push((data, 0))
        _pending_writes()
      end
    end

  fun ref _complete_writes(len: U32) =>
    """
    The OS has informed as that len bytes of pending writes have completed.
    This occurs only with IOCP on Windows.
    """
    ifdef windows then
      var rem = len.usize()

      if rem == 0 then
        // IOCP reported a failed write on this chunk. Non-graceful shutdown.
        try _pending.shift()? end
        _hard_close()
        return
      end

      while rem > 0 do
        try
          let node = _pending.head()?
          (let data, let offset) = node()?
          let total = rem + offset

          if total < data.size() then
            node()? = (data, total)
            rem = 0
          else
            _pending.shift()?
            rem = total - data.size()
          end
        end
      end

      if _pending.size() < 16 then
        // If fewer than 16 asynchronous writes are scheduled, remove
        // backpressure. The choice of 16 is rather arbitrary and probably
        // needs to be tuned.
        _release_backpressure()
      end
    end

  fun ref _pending_writes(): Bool =>
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
      while _writeable and not _shutdown_peer and (_pending_writev_total > 0) do
        try
          //determine number of bytes and buffers to send
          if (_pending_writev.size()/2) < writev_batch_size then
            num_to_send = _pending_writev.size()/2
            bytes_to_send = _pending_writev_total
          else
            //have more buffers than a single writev can handle
            //iterate over buffers being sent to add up total
            num_to_send = writev_batch_size
            bytes_to_send = 0
            for d in Range[USize](1, num_to_send*2, 2) do
              bytes_to_send = bytes_to_send + _pending_writev(d)?
            end
          end

          // Write as much data as possible.
          var len = @pony_os_writev[USize](_event,
            _pending_writev.cpointer(), num_to_send) ?

          if len < bytes_to_send then
            while len > 0 do
              let iov_p = _pending_writev(0)?
              let iov_s = _pending_writev(1)?
              if iov_s <= len then
                len = len - iov_s
                _pending_writev.shift()?
                _pending_writev.shift()?
                _pending.shift()?
                _pending_writev_total = _pending_writev_total - iov_s
              else
                _pending_writev.update(0, iov_p+len)?
                _pending_writev.update(1, iov_s-len)?
                _pending_writev_total = _pending_writev_total - len
                len = 0
              end
            end
            _apply_backpressure()
          else
            // sent all data we requested in this batch
            _pending_writev_total = _pending_writev_total - bytes_to_send
            if _pending_writev_total == 0 then
              _pending_writev.clear()
              _pending.clear()
              return true
            else
              for d in Range[USize](0, num_to_send, 1) do
                _pending_writev.shift()?
                _pending_writev.shift()?
                _pending.shift()?
              end
            end
          end
        else
          // Non-graceful shutdown on error.
          _hard_close()
        end
      end
    end

    false

  fun ref _complete_reads(len: U32) =>
    """
    The OS has informed as that len bytes of pending reads have completed.
    This occurs only with IOCP on Windows.
    """
    ifdef windows then
      match len.usize()
      | 0 =>
        // The socket has been closed from the other side, or a hard close has
        // cancelled the queued read.
        _readable = false
        _shutdown_peer = true
        _hard_close()
        return
      | _next_size =>
        _next_size = _max_size.min(_next_size * 2)
      end

      _read_len = _read_len + len.usize()

      if (not _muted) and (_read_len >= _expect) then
        let data = _read_buf = recover Array[U8] end
        data.truncate(_read_len)
        _read_len = 0

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
      _read_buf.undefined(_expect)
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
          _read_buf.cpointer().usize() + _read_len,
          _read_buf.size() - _read_len) ?
      else
        _hard_close()
      end
    end

  fun ref pending_reads() =>
    """
    Unless this connection is currently muted, read while data is available,
    guessing the next packet length as we go. If we read 4 kb of data, send
    ourself a resume message and stop reading, to avoid starving other actors.
    """
    ifdef not windows then
      try
        var sum: USize = 0
        var received_called: USize = 0

        while _readable and not _shutdown_peer do
          if _muted then
            return
          end

          // Read as much data as possible.
          let len = @pony_os_recv[USize](
            _event,
            _read_buf.cpointer().usize() + _read_len,
            _read_buf.size() - _read_len) ?

          match len
          | 0 =>
            // Would block, try again later.
            ifdef linux then
              // this is safe because asio thread isn't currently subscribed
              // for a read event so will not be writing to the readable flag
              @pony_asio_event_set_readable[None](_event, false)
              _readable = false
              @pony_asio_event_resubscribe_read(_event)
            else
              _readable = false
            end
            return
          | _next_size =>
            // Increase the read buffer size.
            _next_size = _max_size.min(_next_size * 2)
          end

          _read_len = _read_len + len

          if _read_len >= _expect then
            let data = _read_buf = recover Array[U8] end
            data.truncate(_read_len)
            _read_len = 0

            received_called = received_called + 1
            if not notify.received(_conn, consume data,
              received_called)
            then
              _read_buf_size()
              _conn._read_again()
              return
            else
              _read_buf_size()
            end

            sum = sum + len

            if sum >= _max_size then
              // If we've read _max_size, yield and read again later.
              _conn._read_again()
              return
            end
          end
        end
      else
        // The socket has been closed from the other side.
        _shutdown_peer = true
        _hard_close()
      end
    end

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

  fun ref close() =>
    """
    Attempt to perform a graceful shutdown. Don't accept new writes. If the
    connection isn't muted then we won't finish closing until we get a zero
    length read.  If the connection is muted, perform a hard close and
    shut down immediately.
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

    let rem = ifdef windows then
      _pending.size()
    else
      _pending_writev_total
    end

    if
      not _shutdown and
      (_connect_count == 0) and
      (rem == 0)
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
      if not _connected and not _readable and (_pending.size() == 0) then
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

    ifdef not windows then
      // Unsubscribe immediately and drop all pending writes.
      @pony_asio_event_unsubscribe(_event)
      _pending_writev.clear()
      _pending.clear()
      _pending_writev_total = 0
      _readable = false
      _writeable = false
      ifdef linux then
        @pony_asio_event_set_readable[None](_event, false)
        @pony_asio_event_set_writeable[None](_event, false)
      end
    end

    // On windows, this will also cancel all outstanding IOCP operations.
    @pony_os_socket_close[None](_fd)
    _fd = -1

    notify.closed(_conn)

// TODO: Uncomment when this is a full replacement for net package and includes
// the listener stuff
//    try (_listen as TCPListener)._conn_closed() end

  fun ref _apply_backpressure() =>
    ifdef not windows then
      _writeable = false
      ifdef linux then
        // this is safe because asio thread isn't currently subscribed
        // for a write event so will not be writing to the readable flag
        @pony_asio_event_set_writeable[None](_event, false)
        @pony_asio_event_resubscribe_write(_event)
      end
    end

    notify.throttled(_conn)

  fun ref _release_backpressure() =>
    notify.unthrottled(_conn)

  fun ref reconnect() =>
    _connect_count = @pony_os_connect_tcp[U32](this,
      _host.cstring(), _service.cstring(),
      _from.cstring())
    _notify_connecting()
