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

  be _write_again() =>
    """
    Resume writes.
    """
    get_handler().pending_writes()

  fun ref expect(qty: USize = 0) =>
    """
    A `received` call on the notifier must contain exactly `qty` bytes. If
    `qty` is zero, the call can contain any amount of data. This has no effect
    if called in the `sent` notifier callback.
    """
    get_handler().expect(qty)

  be reconnect() =>
    get_handler().reconnect()

