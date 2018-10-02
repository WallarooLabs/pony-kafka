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
use "itertools"

class RewindableValReader is RewindableReader
  """
  Store network data and provide a parsing interface.

  `Reader` provides a way to extract typed data from a sequence of
  bytes. The `Reader` manages the underlying data structures to
  provide a read cursor over a contiguous sequence of bytes. It is
  useful for decoding data that is received over a network or stored
  in a file. Chunk of bytes are added to the `Reader` using the
  `append` method, and typed data is extracted using the getter
  methods.

  For example, suppose we have a UDP-based network data protocol where
  messages consist of the following:

  * `list_size` - the number of items in the following list of items
    as a big-endian 32-bit integer
  * zero or more items of the following data:
    * a big-endian 64-bit floating point number
    * a string that starts with a big-endian 32-bit integer that
      specifies the length of the string, followed by a number of
      bytes that represent the string

  A message would be something like this:

  ```
  [message_length][list_size][float1][string1][float2][string2]...
  ```

  The following program uses a `Reader` to decode a message of
  this type and print them:

  ```
  use "net"
  use "collections"

  class Notify is StdinNotify
    let _env: Env
    new create(env: Env) =>
      _env = env
    fun ref apply(data: Array[U8] iso) =>
      let rb = Reader
      rb.append(consume data)
      try
        while true do
          let len = rb.i32_be()
          let items = rb.i32_be().usize()
          for range in Range(0, items) do
            let f = rb.f32_be()
            let str_len = rb.i32_be().usize()
            let str = String.from_array(rb.block(str_len))
            _env.out.print("[(" + f.string() + "), (" + str + ")]")
          end
        end
      end

  actor Main
    new create(env: Env) =>
      env.input(recover Notify(env) end, 1024)
  ```
  """
  embed _chunks: Array[Array[U8] val] = _chunks.create()
  var _available: USize = 0
  var _total_size: USize = 0
  var _current_position: USize = 0
  var _current_node: USize = -1
  var _current_node_offset: USize = 0

  fun size(): USize =>
    """
    Return the number of available bytes.
    """
    _available

  fun ref clear() =>
    """
    Discard all pending data.
    """
    _chunks.clear()
    _available = 0
    _total_size = 0
    _current_position = 0
    _current_node_offset = 0
    _current_node = -1

  fun ref set_position(pos: USize) ? =>
    """
    Set new abosolute position in rewindable buffer.
    """
    if pos <= _total_size then
      _current_position = 0
      _current_node_offset = 0
      _available = _total_size
      _current_node = 0
      skip(pos)?
    else
      error
    end

  fun current_position(): USize =>
    """
    Current position in rewindable buffer.
    """
    _current_position

  fun total_size(): USize =>
    """
    Total size of rewindable buffer.
    """
    _total_size

  fun ref _increment_position(num_bytes: USize) =>
    """
    Manage available bytes/current position
    """
    _available = _available - num_bytes
    _current_position = _current_position + num_bytes

  fun ref _switch_node() =>
    """
    Switch to the next node and update current node offset
    """
    _current_node = _current_node + 1
    _current_node_offset = 0

  fun ref _append(data: ByteSeq) =>
    """
    Add a chunk of data.
    """
    let data_array =
      match data
      | let data': Array[U8] val => data'
      | let data': String => data'.array()
      end

    _available = _available + data_array.size()
    _total_size = _total_size + data_array.size()
    _chunks.push(data_array)

    if _current_node == -1 then
      _current_node = _chunks.size() - 1
    end

  fun ref append(data: (ByteSeq | Array[ByteSeq] val)) =>
    """
    Add a chunk of data.
    """
    match data
    | let data': ByteSeq => _append(data')
    | let data': Array[ByteSeq] val =>
      for d in data'.values() do
        _append(d)
      end
    end

  fun ref append_val(data: (ByteSeq | Array[ByteSeq] val)) =>
    append(data)

  fun ref append_iso(data: (ByteSeq iso | Array[ByteSeq iso] iso)) =>
    match consume data
    | let data': ByteSeq iso => _append(consume data')
    | let data': Array[ByteSeq iso] iso =>
      let tmp_data = consume ref data'
      // reverse in place to avoid having to do expensive shifts for each element
      tmp_data.reverse_in_place()
      while tmp_data.size() > 0 do
        try
          let d = tmp_data.pop()?
          _append(consume d)
        else
          break
        end
      end
    end

  fun ref skip(n: USize) ? =>
    """
    Skip n bytes.
    """
    if _available >= n then
      _increment_position(n)

      var rem = n

      while rem > 0 do
        var data = _chunks(_current_node)?
        let avail = data.size() - _current_node_offset

        if avail > rem then
          _current_node_offset = _current_node_offset + rem
          break
        end

        rem = rem - avail
        _switch_node()
      end
    else
      error
    end

  fun ref read_block(len: USize): Array[U8] iso^ ? =>
    """
    Return a block as a contiguous chunk of memory.
    """
    (let num_bytes, let data) = _read_bytes(len)?

    match data
    | let a: Array[U8] val =>
      recover a.clone() end
    | let arr: Array[Array[U8] val] val =>
      var out = recover Array[U8].>undefined(num_bytes) end
      var i: USize = 0
      for a in arr.values() do
        out = recover
          let r = consume ref out
          a.copy_to(r, 0, i, a.size())
          i = i + a.size()
          consume r
        end
      end
      out
    end

  fun ref read_until(separator: U8): Array[U8] iso^ ? =>
    """
    Find the first occurence of the separator and return the block of bytes
    before its position. The separator is not included in the returned array,
    but it is removed from the buffer. To read a line of text, prefer line()
    that handles \n and \r\n.
    """
    let b = read_block(_distance_of(separator)? - 1)?
    read_byte()?
    consume b

  fun ref read_line(keep_line_breaks: Bool = false): String iso^ ? =>
    """
    Return a \n or \r\n terminated line as a string. The newline is not
    included in the returned string, but it is removed from the network buffer.
    Set `keep_line_breaks` to `true` to keep the line breaks in the returned line.
    """
    let len = _distance_of('\n')?

    let out = read_block(len)?

    let trunc_len: USize =
      if keep_line_breaks then
        0
      elseif (len >= 2) and (out(len - 2)? == '\r') then
        2
      else
        1
      end
    out.truncate(len - trunc_len)

    recover String.from_iso_array(consume out) end

  fun ref read_u8(): U8 ? =>
    """
    Read a U8.
    """
    read_byte()?

  fun ref read_u16(): U16 ? =>
    """
    Read a U16.
    """
    let num_bytes = U16(0).bytewidth()
    if _available < num_bytes then
      error
    end
    var data = _chunks(_current_node)?
    if (data.size() - _current_node_offset) >= num_bytes then
      let r = data.read_u16(_current_node_offset)?

      _current_node_offset = _current_node_offset + num_bytes
      _increment_position(num_bytes)
      if _current_node_offset >= data.size() then
        _switch_node()
      end
      r
    else
      // single array did not have all the bytes needed
      ifdef bigendian then
        (read_u8()?.u16() << 8) or read_u8()?.u16()
      else
         read_u8()?.u16() or (read_u8()?.u16() << 8)
      end
    end

  fun ref read_u32(): U32 ? =>
    """
    Read a U32.
    """
    let num_bytes = U32(0).bytewidth()
    if _available < num_bytes then
      error
    end
    var data = _chunks(_current_node)?
    if (data.size() - _current_node_offset) >= num_bytes then
      let r = data.read_u32(_current_node_offset)?

      _current_node_offset = _current_node_offset + num_bytes
      _increment_position(num_bytes)
      if _current_node_offset >= data.size() then
        _switch_node()
      end
      r
    else
      // single array did not have all the bytes needed
      ifdef bigendian then
        (read_u8()?.u32() << 24) or (read_u8()?.u32() << 16) or
          (read_u8()?.u32() << 8) or read_u8()?.u32()
      else
        read_u8()?.u32() or (read_u8()?.u32() << 8) or
          (read_u8()?.u32() << 16) or (read_u8()?.u32() << 24)
      end
    end

  fun ref read_u64(): U64 ? =>
    """
    Read a U64.
    """
    let num_bytes = U64(0).bytewidth()
    if _available < num_bytes then
      error
    end
    var data = _chunks(_current_node)?
    if (data.size() - _current_node_offset) >= num_bytes then
      let r = data.read_u64(_current_node_offset)?

      _current_node_offset = _current_node_offset + num_bytes
      _increment_position(num_bytes)
      if _current_node_offset >= data.size() then
        _switch_node()
      end
      r
    else
      // single array did not have all the bytes needed
      ifdef bigendian then
        (read_u8()?.u64() << 56) or (read_u8()?.u64() << 48) or
          (read_u8()?.u64() << 40) or (read_u8()?.u64() << 32) or
          (read_u8()?.u64() << 24) or (read_u8()?.u64() << 16) or
          (read_u8()?.u64() << 8) or read_u8()?.u64()
      else
        read_u8()?.u64() or (read_u8()?.u64() << 8) or
          (read_u8()?.u64() << 16) or (read_u8()?.u64() << 24) or
          (read_u8()?.u64() << 32) or (read_u8()?.u64() << 40) or
          (read_u8()?.u64() << 48) or (read_u8()?.u64() << 56)
      end
    end

  fun ref read_u128(): U128 ? =>
    """
    Read a U128.
    """
    let num_bytes = U128(0).bytewidth()
    if _available < num_bytes then
      error
    end
    var data = _chunks(_current_node)?
    if (data.size() - _current_node_offset) >= num_bytes then
      let r = data.read_u128(_current_node_offset)?

      _current_node_offset = _current_node_offset + num_bytes
      _increment_position(num_bytes)
      if _current_node_offset >= data.size() then
        _switch_node()
      end
      r
    else
      // single array did not have all the bytes needed
      ifdef bigendian then
        (read_u8()?.u128() << 120) or (read_u8()?.u128() << 112) or
          (read_u8()?.u128() << 104) or (read_u8()?.u128() << 96) or
          (read_u8()?.u128() << 88) or (read_u8()?.u128() << 80) or
          (read_u8()?.u128() << 72) or (read_u8()?.u128() << 64) or
          (read_u8()?.u128() << 56) or (read_u8()?.u128() << 48) or
          (read_u8()?.u128() << 40) or (read_u8()?.u128() << 32) or
          (read_u8()?.u128() << 24) or (read_u8()?.u128() << 16) or
          (read_u8()?.u128() << 8) or read_u8()?.u128()
      else
        read_u8()?.u128() or (read_u8()?.u128() << 8) or
          (read_u8()?.u128() << 16) or (read_u8()?.u128() << 24) or
          (read_u8()?.u128() << 32) or (read_u8()?.u128() << 40) or
          (read_u8()?.u128() << 48) or (read_u8()?.u128() << 56) or
          (read_u8()?.u128() << 64) or (read_u8()?.u128() << 72) or
          (read_u8()?.u128() << 80) or (read_u8()?.u128() << 88) or
          (read_u8()?.u128() << 96) or (read_u8()?.u128() << 104) or
          (read_u8()?.u128() << 112) or (read_u8()?.u128() << 120)
      end
    end

  fun ref read_byte(): U8 ? =>
    """
    Get a single byte.
    """
    let num_bytes = U8(0).bytewidth()
    var data = _chunks(_current_node)?
    let r = data.read_u8(_current_node_offset)?

    _current_node_offset = _current_node_offset + num_bytes
    _increment_position(num_bytes)

    if _current_node_offset >= data.size() then
      _switch_node()
    end
    r

  fun ref read_bytes(len: USize): (Array[U8] val | Array[Array[U8] val] val) ?
  =>
    _read_bytes(len)?._2

  fun ref _read_bytes(len: USize): (USize, (Array[U8] val | Array[Array[U8] val] val)) ?
  =>
    """
    Return a number of bytes as either a contiguous array or an array of arrays
    """
    if len == 0 then
      return (0, recover Array[U8] end)
    end

    if _available < len then
      error
    end

    _increment_position(len)
    var out = recover Array[Array[U8] val] end
    var i = USize(0)

    while i < len do
      let data = _chunks(_current_node)?

      let avail = data.size() - _current_node_offset
      let need = len - i
      let copy_len = need.min(avail)

      let next_segment = data.trim(_current_node_offset, _current_node_offset + copy_len)

      if avail > need then
        _current_node_offset = _current_node_offset + need
        if out.size() == 0 then
          return (copy_len, next_segment)
        else
          out.push(next_segment)
          break
        end
      else
        out.push(next_segment)
      end

      i = i + copy_len
      _switch_node()
    end

    (i, consume out)

  fun ref read_contiguous_bytes(len: USize): Array[U8] val ? =>
    """
    Return a block as a contiguous chunk of memory without copying if possible
    or copy together multiple chunks if required.
    """
    if len == 0 then
      return recover Array[U8] end
    end

    if _available < len then
      error
    end

    let data = _chunks(_current_node)?

    let avail = data.size() - _current_node_offset
    let need = len
    let copy_len = need.min(avail)

    if avail < len then
      return read_block(len)?
    end

    if avail >= need then
      let next_segment = data.trim(_current_node_offset, _current_node_offset + copy_len)
      _current_node_offset = _current_node_offset + need
      _increment_position(len)
      return next_segment
    end

    error

  fun ref peek_u8(offset: USize = 0): U8 ? =>
    """
    Get the U8 at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    peek_byte(offset)?

  fun ref peek_u16(offset: USize = 0): U16 ? =>
    """
    Get the U16 at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    let num_bytes = U16(0).bytewidth()
    if _available < (offset + num_bytes) then
      error
    end
    try
      var offset' = offset + _current_node_offset
      var current_node' = _current_node

      while true do
        var data = _chunks(current_node')?

        let data_size = data.size()
        if offset' >= data_size then
          offset' = offset' - data_size
          current_node' = current_node' + 1
        else
          return data.read_u16(offset')?
        end
      end

      error
    else
      // single array did not have all the bytes needed
      ifdef bigendian then
        (peek_u8(offset)?.u16() << 8) or peek_u8(offset + 1)?.u16()
      else
         peek_u8(offset)?.u16() or (peek_u8(offset + 1)?.u16() << 8)
      end
    end

  fun ref peek_u32(offset: USize = 0): U32 ? =>
    """
    Get the U32 at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    let num_bytes = U32(0).bytewidth()
    if _available < (offset + num_bytes) then
      error
    end
    try
      var offset' = offset + _current_node_offset
      var current_node' = _current_node

      while true do
        var data = _chunks(current_node')?

        let data_size = data.size()
        if offset' >= data_size then
          offset' = offset' - data_size
          current_node' = current_node' + 1
        else
          return data.read_u32(offset')?
        end
      end

      error
    else
      // single array did not have all the bytes needed
      ifdef bigendian then
        (peek_u8(offset)?.u32() << 24) or (peek_u8(offset + 1)?.u32() << 16) or
          (peek_u8(offset + 2)?.u32() << 8) or peek_u8(offset + 3)?.u32()
      else
        peek_u8(offset)?.u32() or (peek_u8(offset + 1)?.u32() << 8) or
          (peek_u8(offset + 2)?.u32() << 16) or (peek_u8(offset + 3)?.u32() << 24)
      end
    end

  fun ref peek_u64(offset: USize = 0): U64 ? =>
    """
    Get the U64 at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    let num_bytes = U64(0).bytewidth()
    if _available < (offset + num_bytes) then
      error
    end
    try
      var offset' = offset + _current_node_offset
      var current_node' = _current_node

      while true do
        var data = _chunks(current_node')?

        let data_size = data.size()
        if offset' >= data_size then
          offset' = offset' - data_size
          current_node' = current_node' + 1
        else
          return data.read_u64(offset')?
        end
      end

      error
    else
      // single array did not have all the bytes needed
      ifdef bigendian then
        (peek_u8(offset)?.u64() << 56) or (peek_u8(offset + 1)?.u64() << 48) or
          (peek_u8(offset + 2)?.u64() << 40) or (peek_u8(offset + 3)?.u64() << 32) or
          (peek_u8(offset + 4)?.u64() << 24) or (peek_u8(offset + 5)?.u64() << 16) or
          (peek_u8(offset + 6)?.u64() << 8) or peek_u8(offset + 7)?.u64()
      else
        peek_u8(offset)?.u64() or (peek_u8(offset + 1)?.u64() << 8) or
          (peek_u8(offset + 2)?.u64() << 16) or (peek_u8(offset + 3)?.u64() << 24) or
          (peek_u8(offset + 4)?.u64() << 32) or (peek_u8(offset + 5)?.u64() << 40) or
          (peek_u8(offset + 6)?.u64() << 48) or (peek_u8(offset + 7)?.u64() << 56)
      end
    end

  fun ref peek_u128(offset: USize = 0): U128 ? =>
    """
    Get the U128 at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    let num_bytes = U128(0).bytewidth()
    if _available < (offset + num_bytes) then
      error
    end
    try
      var offset' = offset + _current_node_offset
      var current_node' = _current_node

      while true do
        var data = _chunks(current_node')?

        let data_size = data.size()
        if offset' >= data_size then
          offset' = offset' - data_size
          current_node' = current_node' + 1
        else
          return data.read_u128(offset')?
        end
      end

      error
    else
      // single array did not have all the bytes needed
      ifdef bigendian then
        (peek_u8(offset)?.u128() << 120) or (peek_u8(offset + 1)?.u128() << 112) or
          (peek_u8(offset + 2)?.u128() << 104) or (peek_u8(offset + 3)?.u128() << 96) or
          (peek_u8(offset + 4)?.u128() << 88) or (peek_u8(offset + 5)?.u128() << 80) or
          (peek_u8(offset + 6)?.u128() << 72) or (peek_u8(offset + 7)?.u128() << 64) or
          (peek_u8(offset + 8)?.u128() << 56) or (peek_u8(offset + 9)?.u128() << 48) or
          (peek_u8(offset + 10)?.u128() << 40) or (peek_u8(offset + 11)?.u128() << 32) or
          (peek_u8(offset + 12)?.u128() << 24) or (peek_u8(offset + 13)?.u128() << 16) or
          (peek_u8(offset + 14)?.u128() << 8) or peek_u8(offset + 15)?.u128()
      else
        peek_u8(offset)?.u128() or (peek_u8(offset + 1)?.u128() << 8) or
          (peek_u8(offset + 2)?.u128() << 16) or (peek_u8(offset + 3)?.u128() << 24) or
          (peek_u8(offset + 4)?.u128() << 32) or (peek_u8(offset + 5)?.u128() << 40) or
          (peek_u8(offset + 6)?.u128() << 48) or (peek_u8(offset + 7)?.u128() << 56) or
          (peek_u8(offset + 8)?.u128() << 64) or (peek_u8(offset + 9)?.u128() << 72) or
          (peek_u8(offset + 10)?.u128() << 80) or (peek_u8(offset + 11)?.u128() << 88) or
          (peek_u8(offset + 12)?.u128() << 96) or (peek_u8(offset + 13)?.u128() << 104) or
          (peek_u8(offset + 14)?.u128() << 112) or (peek_u8(offset + 15)?.u128() << 120)
      end
    end

  fun ref peek_byte(offset: USize = 0): U8 ? =>
    """
    Get the byte at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    if _available < (offset + 1) then
      error
    end

    var offset' = offset + _current_node_offset
    var current_node' = _current_node

    while true do
      var data = _chunks(current_node')?

      let data_size = data.size()
      if offset' >= data_size then
        offset' = offset' - data_size
        current_node' = current_node' + 1
      else
        return data(offset')?
      end
    end

    error

  fun ref peek_bytes(len: USize, offset: USize = 0):
    (Array[U8] val | Array[Array[U8] val] val) ?
  =>
    """
    Return a number of bytes as either a contiguous array or an array of arrays
    without moving the cursor forward
    """
    if _available < (offset + len) then
      error
    end

    var offset' = offset + _current_node_offset
    var current_node' = _current_node

    while true do
      var data = _chunks(current_node')?

      let data_size = data.size()
      if offset' >= data_size then
        offset' = offset' - data_size
        current_node' = current_node' + 1
      else
        if (data_size - offset') > len then
          return data.trim(offset', offset' + len)
        end

        var out = recover Array[Array[U8] val] end
        var i = USize(0)

        i = i + (data_size - offset')
        out.push(data.trim(offset'))

        while i < len do
          current_node' = current_node' + 1
          let data' = _chunks(current_node')?

          let avail = data'.size()
          let need = len - i
          let copy_len = need.min(avail)

          let next_segment = data'.trim(0, copy_len)

          if avail > need then
            if out.size() == 0 then
              return next_segment
            else
              out.push(next_segment)
              break
            end
          else
            out.push(next_segment)
          end

          i = i + copy_len
        end

        return consume out

      end
    end

    error

  fun ref peek_contiguous_bytes(len: USize, offset: USize = 0): Array[U8] val ? =>
    """
    Return a block as a contiguous chunk of memory without copying if possible
    or copy together multiple chunks if required.
    """
    if _available < (offset + len) then
      error
    end

    var offset' = offset + _current_node_offset
    var current_node' = _current_node

    while true do
      var data = _chunks(current_node')?

      let data_size = data.size()
      if offset' >= data_size then
        offset' = offset' - data_size
      else
        if (data_size - offset') > len then
          return data.trim(offset', offset' + len)
        end

        let data' = peek_bytes(len, offset)?

        match data'
        | let a: Array[U8] val =>
          return a
        | let arr: Array[Array[U8] val] val =>
          var out = recover Array[U8].>undefined(len) end
          var i: USize = 0
          for a in arr.values() do
            out = recover
              let r = consume ref out
              a.copy_to(r, 0, i, a.size())
              i = i + a.size()
              consume r
            end
          end
          return out
        end

      end

      current_node' = current_node' + 1
    end

    error

  fun ref peek_block(len: USize, offset: USize = 0): Array[U8] iso^ ? =>
    """
    Return a block as a contiguous chunk of memory.
    """
    let num_bytes = len
    let data = peek_bytes(len, offset)?

    match data
    | let a: Array[U8] val =>
      recover a.clone() end
    | let arr: Array[Array[U8] val] val =>
      var out = recover Array[U8].>undefined(num_bytes) end
      var i: USize = 0
      for a in arr.values() do
        out = recover
          let r = consume ref out
          a.copy_to(r, 0, i, a.size())
          i = i + a.size()
          consume r
        end
      end
      out
    end

  fun ref peek_until(separator: U8, offset: USize = 0): Array[U8] iso^ ? =>
    """
    Find the first occurrence of the separator and return the block of bytes
    before its position. The separator is not included in the returned array,
    but it is removed from the buffer. To read a line of text, prefer line()
    that handles \n and \r\n.
    """
    peek_block(_distance_of(separator, offset)? - 1, offset)?

  fun ref peek_line(keep_line_breaks: Bool = false, offset: USize = 0): String iso^ ? =>
    """
    Return a \n or \r\n terminated line as a string. By default the newline is not
    included in the returned string, but it is removed from the buffer.
    Set `keep_line_breaks` to `true` to keep the line breaks in the returned line.
    """
    let len = _distance_of('\n', offset)?

    let out = peek_block(len, offset)?

    let trunc_len: USize =
      if keep_line_breaks then
        0
      elseif (len >= 2) and (out(len - 2)? == '\r') then
        2
      else
        1
      end
    out.truncate(len - trunc_len)

    recover String.from_iso_array(consume out) end

  fun ref _distance_of(byte: U8, offset: USize = 0): USize ? =>
    """
    Get the distance to the first occurence of the given byte
    """
    if _chunks.size() == 0 then
      error
    end

    var search_len: USize = 0
    var offset' = offset + _current_node_offset
    var current_node': USize = _current_node

    if offset' > 0 then

      while true do
        var data = _chunks(current_node')?

        let data_size = data.size()
        if offset' >= data_size then
          offset' = offset' - data_size
          current_node' = current_node' + 1
        else
          break
        end
      end

    end

    while true do
      try
        let len = (search_len + _chunks(current_node')?.find(byte, offset')? + 1) - offset'
        search_len = 0
        return len
      end

      search_len = search_len + (_chunks(current_node')?.size() - offset')


      if current_node' > _chunks.size() then
        break
      end

      current_node' = current_node' + 1
      offset' = 0
    end

    error
