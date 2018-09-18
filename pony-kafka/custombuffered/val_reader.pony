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

class ValReader is PeekableReader
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
  embed _chunks: List[(Array[U8] val, USize)] = _chunks.create()
  var _available: USize = 0
  var _search_node: (ListNode[(Array[U8] val, USize)] | None) = None
  var _search_len: USize = 0

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
    _chunks.push((data_array, 0))

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

  fun ref skip(n: USize) ? =>
    """
    Skip n bytes.
    """
    if _available >= n then
      _available = _available - n
      var rem = n

      while rem > 0 do
        let node = _chunks.head()?
        (var data, var offset) = node()?
        let avail = data.size() - offset

        if avail > rem then
          node()? = (data, offset + rem)
          break
        end

        rem = rem - avail
        _chunks.shift()?
      end

    else
      error
    end

  fun ref block(len: USize): Array[U8] iso^ ? =>
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
    let b = block(_distance_of(separator)? - 1)?
    read_byte()?
    consume b

  fun ref line(): String ? =>
    """
    Return a \n or \r\n terminated line as a string. The newline is not
    included in the returned string, but it is removed from the network buffer.
    """
    let len = _search_length()?

    _available = _available - len
    var out = recover String(len) end
    var i = USize(0)

    while i < len do
      let node = _chunks.head()?
      (let data, let offset) = node()?

      let avail = data.size() - offset
      let need = len - i
      let copy_len = need.min(avail)

      out.append(data, offset, copy_len)

      if avail > need then
        node()? = (data, offset + need)
        break
      end

      i = i + copy_len
      _chunks.shift()?
    end

    out.truncate(len -
      if (len >= 2) and (out.at_offset(-2)? == '\r') then 2 else 1 end)

    out

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
    try
      let node = _chunks.head()?
      (var data, var offset) = node()?
      let r = data.read_u16(offset)?

      offset = offset + num_bytes
      _available = _available - num_bytes
      if offset < data.size() then
        node()? = (data, offset)
      else
        _chunks.shift()?
      end
      r
    else
      let data = read_bytes(num_bytes)?

      _decode_u16(data)?
    end

  fun _decode_u16(data: (Array[U8] val | Array[Array[U8] val] val)): U16 ? =>
    match data
    | let d: Array[U8] val =>
      d.read_u16(0)?
    | let d: (Array[Array[U8] val] val) =>
      _decode_u16_array(d)?
    end

  fun _decode_u16_array(data: Array[Array[U8] val] val): U16 ? =>
    var out: U16 = 0
    let iters = Array[Iterator[U8]]
    for a in data.values() do
      iters.push(a.values())
    end
    var i: U16 = 0
    let iter_all = Iter[U8].chain(iters.values())
    while iter_all.has_next() do
      ifdef bigendian then
        out = (out << 8) or iter_all.next()?.u16()
      else
        out = out or (iter_all.next()?.u16() << (i * 8))
        i = i + 1
      end
    end
    out

  fun ref read_u32(): U32 ? =>
    """
    Read a U32.
    """
    let num_bytes = U32(0).bytewidth()
    if _available < num_bytes then
      error
    end
    try
      let node = _chunks.head()?
      (var data, var offset) = node()?
      let r = data.read_u32(offset)?

      offset = offset + num_bytes
      _available = _available - num_bytes
      if offset < data.size() then
        node()? = (data, offset)
      else
        _chunks.shift()?
      end
      r
    else
      let data = read_bytes(num_bytes)?

      _decode_u32(data)?
    end

  fun _decode_u32(data: (Array[U8] val | Array[Array[U8] val] val)): U32 ? =>
    match data
    | let d: Array[U8] val =>
      d.read_u32(0)?
    | let d: (Array[Array[U8] val] val) =>
      _decode_u32_array(d)?
    end

  fun _decode_u32_array(data: Array[Array[U8] val] val): U32 ? =>
    var out: U32 = 0
    let iters = Array[Iterator[U8]]
    for a in data.values() do
      iters.push(a.values())
    end
    let iter_all = Iter[U8].chain(iters.values())
    var i: U32 = 0
    while iter_all.has_next() do
      ifdef bigendian then
        out = (out << 8) or iter_all.next()?.u32()
      else
        out = out or (iter_all.next()?.u32() << (i * 8))
        i = i + 1
      end
    end
    out

  fun ref read_u64(): U64 ? =>
    """
    Read a U64.
    """
    let num_bytes = U64(0).bytewidth()
    if _available < num_bytes then
      error
    end
    try
      let node = _chunks.head()?
      (var data, var offset) = node()?
      let r = data.read_u64(offset)?

      offset = offset + num_bytes
      _available = _available - num_bytes
      if offset < data.size() then
        node()? = (data, offset)
      else
        _chunks.shift()?
      end
      r
    else
      let data = read_bytes(num_bytes)?

      _decode_u64(data)?
    end

  fun _decode_u64(data: (Array[U8] val | Array[Array[U8] val] val)): U64 ? =>
    match data
    | let d: Array[U8] val =>
      d.read_u64(0)?
    | let d: (Array[Array[U8] val] val) =>
      _decode_u64_array(d)?
    end

  fun _decode_u64_array(data: Array[Array[U8] val] val): U64 ? =>
    var out: U64 = 0
    let iters = Array[Iterator[U8]]
    for a in data.values() do
      iters.push(a.values())
    end
    let iter_all = Iter[U8].chain(iters.values())
    var i: U64 = 0
    while iter_all.has_next() do
      ifdef bigendian then
        out = (out << 8) or iter_all.next()?.u64()
      else
        out = out or (iter_all.next()?.u64() << (i * 8))
        i = i + 1
      end
    end
    out

  fun ref read_u128(): U128 ? =>
    """
    Read a U128.
    """
    let num_bytes = U128(0).bytewidth()
    if _available < num_bytes then
      error
    end
    try
      let node = _chunks.head()?
      (var data, var offset) = node()?
      let r = data.read_u128(offset)?

      offset = offset + num_bytes
      _available = _available - num_bytes
      if offset < data.size() then
        node()? = (data, offset)
      else
        _chunks.shift()?
      end
      r
    else
      let data = read_bytes(num_bytes)?

      _decode_u128(data)?
    end

  fun _decode_u128(data: (Array[U8] val | Array[Array[U8] val] val)): U128 ? =>
    match data
    | let d: Array[U8] val =>
      d.read_u128(0)?
    | let d: (Array[Array[U8] val] val) =>
      _decode_u128_array(d)?
    end

  fun _decode_u128_array(data: Array[Array[U8] val] val): U128 ? =>
    var out: U128 = 0
    let iters = Array[Iterator[U8]]
    for a in data.values() do
      iters.push(a.values())
    end
    let iter_all = Iter[U8].chain(iters.values())
    var i: U128 = 0
    while iter_all.has_next() do
      ifdef bigendian then
        out = (out << 8) or iter_all.next()?.u128()
      else
        out = out or (iter_all.next()?.u128() << (i * 8))
        i = i + 1
      end
    end
    out

  fun ref read_byte(): U8 ? =>
    """
    Get a single byte.
    """
    let num_bytes = U8(0).bytewidth()
    let node = _chunks.head()?
    (var data, var offset) = node()?
    let r = data(offset)?

    offset = offset + num_bytes
    _available = _available - num_bytes

    if offset < data.size() then
      node()? = (data, offset)
    else
      _chunks.shift()?
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

    _available = _available - len
    var out = recover Array[Array[U8] val] end
    var i = USize(0)

    while i < len do
      let node = _chunks.head()?
      (let data, let offset) = node()?

      let avail = data.size() - offset
      let need = len - i
      let copy_len = need.min(avail)

      let next_segment = data.trim(offset, offset + copy_len)

      if avail > need then
        node()? = (data, offset + need)
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
      _chunks.shift()?
    end

    (i, consume out)

  // TODO: Add rewind ability
  // TODO: Add get position
  // TODO: Add peek_contiguous_bytes function
  fun ref read_contiguous_bytes(len: USize): Array[U8] val ? =>
    """
    Return a block as a contiguous chunk of memory without copying if possible
    or throw an error.
    """
    // TODO: enhance to fall back to a copy if have non-contiguous data and
    // return an iso? Not possible because iso/val distinction doesn't exist at
    // runtime? Maybe need to enhance callers to be able to work with
    // non-contiguous memory?

    if len == 0 then
      return recover Array[U8] end
    end

    if _available < len then
      error
    end

    var out = recover Array[Array[U8] val] end

    let node = _chunks.head()?
    (let data, let offset) = node()?

    let avail = data.size() - offset
    let need = len
    let copy_len = need.min(avail)

    if avail >= need then
      let next_segment = data.trim(offset, offset + copy_len)
      node()? = (data, offset + need)
      _available = _available - len
      return next_segment
    end

    node()? = (data, offset)
    error

  fun box peek_u8(offset: USize = 0): U8 ? =>
    """
    Get the U8 at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    peek_byte(offset)?

  fun box peek_u16(offset: USize = 0): U16 ? =>
    """
    Get the U16 at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    let num_bytes = U16(0).bytewidth()
    if _available < (offset + num_bytes) then
      error
    end
    try
      var offset' = offset
      var iter = _chunks.nodes()

      while true do
        let node = iter.next()?
        (var data, var node_offset) = node()?
        offset' = offset' + node_offset

        let data_size = data.size()
        if offset' >= data_size then
          offset' = offset' - data_size
        else
          return data.read_u16(offset')?
        end
      end

      error
    else
      let data = peek_bytes(num_bytes, offset)?

      _decode_u16(data)?
    end

  fun box peek_u32(offset: USize = 0): U32 ? =>
    """
    Get the U32 at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    let num_bytes = U32(0).bytewidth()
    if _available < (offset + num_bytes) then
      error
    end
    try
      var offset' = offset
      var iter = _chunks.nodes()

      while true do
        let node = iter.next()?
        (var data, var node_offset) = node()?
        offset' = offset' + node_offset

        let data_size = data.size()
        if offset' >= data_size then
          offset' = offset' - data_size
        else
          return data.read_u32(offset')?
        end
      end

      error
    else
      let data = peek_bytes(num_bytes, offset)?

      _decode_u32(data)?
    end

  fun box peek_u64(offset: USize = 0): U64 ? =>
    """
    Get the U64 at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    let num_bytes = U64(0).bytewidth()
    if _available < (offset + num_bytes) then
      error
    end
    try
      var offset' = offset
      var iter = _chunks.nodes()

      while true do
        let node = iter.next()?
        (var data, var node_offset) = node()?
        offset' = offset' + node_offset

        let data_size = data.size()
        if offset' >= data_size then
          offset' = offset' - data_size
        else
          return data.read_u64(offset')?
        end
      end

      error
    else
      let data = peek_bytes(num_bytes, offset)?

      _decode_u64(data)?
    end

  fun box peek_u128(offset: USize = 0): U128 ? =>
    """
    Get the U128 at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    let num_bytes = U128(0).bytewidth()
    if _available < (offset + num_bytes) then
      error
    end
    try
      var offset' = offset
      var iter = _chunks.nodes()

      while true do
        let node = iter.next()?
        (var data, var node_offset) = node()?
        offset' = offset' + node_offset

        let data_size = data.size()
        if offset' >= data_size then
          offset' = offset' - data_size
        else
          return data.read_u128(offset')?
        end
      end

      error
    else
      let data = peek_bytes(num_bytes, offset)?

      _decode_u128(data)?
    end

  fun box peek_byte(offset: USize = 0): U8 ? =>
    """
    Get the byte at the given offset without moving the cursor forward.
    Raise an error if the given offset is not yet available.
    """
    if _available < (offset + 1) then
      error
    end

    var offset' = offset
    var iter = _chunks.nodes()

    while true do
      let node = iter.next()?
      (var data, var node_offset) = node()?
      offset' = offset' + node_offset

      let data_size = data.size()
      if offset' >= data_size then
        offset' = offset' - data_size
      else
        return data(offset')?
      end
    end

    error

  fun box peek_bytes(len: USize, offset: USize = 0):
    (Array[U8] val | Array[Array[U8] val] val) ?
  =>
    """
    Return a number of bytes as either a contiguous array or an array of arrays
    without moving the cursor forward
    """
    if _available < (offset + len) then
      error
    end

    var offset' = offset
    var iter = _chunks.nodes()

    while true do
      let node = iter.next()?
      (var data, var node_offset) = node()?
      offset' = offset' + node_offset

      let data_size = data.size()
      if offset' >= data_size then
        offset' = offset' - data_size
      else
        if (data_size - offset') > len then
          return data.trim(offset', offset' + len)
        end

        var out = recover Array[Array[U8] val] end
        var i = USize(0)

        i = i + (data_size - offset')
        out.push(data.trim(offset'))

        while i < len do
          let node' = iter.next()?
          (let data', let offset'') = node'()?

          let avail = data'.size() - offset''
          let need = len - i
          let copy_len = need.min(avail)

          let next_segment = data'.trim(offset'', offset'' + copy_len)

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

  fun ref _distance_of(byte: U8): USize ? =>
    """
    Get the distance to the first occurence of the given byte
    """
    if _chunks.size() == 0 then
      error
    end

    var node = if _search_len > 0 then
      let prev = _search_node as ListNode[(Array[U8] val, USize)]

      if not prev.has_next() then
        error
      end

      prev.next() as ListNode[(Array[U8] val, USize)]
    else
      _chunks.head()?
    end

    while true do
      (var data, var offset) = node()?

      try
        let len = (_search_len + data.find(byte, offset)? + 1) - offset
        _search_node = None
        _search_len = 0
        return len
      end

      _search_len = _search_len + (data.size() - offset)

      if not node.has_next() then
        break
      end

      node = node.next() as ListNode[(Array[U8] val, USize)]
    end

    _search_node = node
    error

  fun ref _search_length(): USize ? =>
    """
    Get the length of a pending line. Raise an error if there is no pending
    line.
    """
    _distance_of('\n')?
