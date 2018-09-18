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

class Writer
  """
  A buffer for building messages.

  `Writer` provides an way to create byte sequences using common
  data encodings. The `Writer` manages the underlying arrays and
  sizes. It is useful for encoding data to send over a network or
  store in a file. Once a message has been built you can call `done()`
  to get the message's `ByteSeq`s, and you can then reuse the
  `Writer` for creating a new message.

  For example, suppose we have a TCP-based network data protocol where
  messages consist of the following:

  * `message_length` - the number of bytes in the message as a
    big-endian 32-bit integer
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

  The following program uses a write buffer to encode an array of
  tuples as a message of this type:

  ```
  use "net"

  actor Main
    new create(env: Env) =>
      let wb = Writer
      let messages = [[(F32(3597.82), "Anderson"), (F32(-7979.3), "Graham")],
                      [(F32(3.14159), "Hopper"), (F32(-83.83), "Jones")]]
      for items in messages.values() do
        wb.i32_be((items.size() / 2).i32())
        for (f, s) in items.values() do
          wb.f32_be(f)
          wb.i32_be(s.size().i32())
          wb.write(s.array())
        end
        let wb_msg = Writer
        wb_msg.i32_be(wb.size().i32())
        wb_msg.writev(wb.done())
        env.out.writev(wb_msg.done())
      end
  ```
  """
  var _chunks: Array[ByteSeq] iso = recover Array[ByteSeq] end
  var _current: Array[U8] iso = recover Array[U8] end
  var _offset: USize = 0
  var _size: USize = 0

  fun ref reserve_current(size': USize) =>
    """
    Reserve space for size bytes in `_current`.
    """
    _check(size')

  fun ref reserve_chunks(size': USize) =>
    """
    Reserve space for size' chunks.

    This needs to be recalled after every call to `done`
    as `done` resets the chunks.
    """
    _chunks.reserve(size')

  fun size(): USize =>
    _size

  fun ref write_byte(data: U8) =>
    """
    Write a byte to the buffer.
    """
    _check(1)
    _byte(data)

  fun ref write_two_bytes(data: U8, data2: U8) =>
    """
    Write a byte to the buffer.
    """
    _check(2)
    _byte(data)
    _byte(data2)

  fun ref write_four_bytes(data: U8, data2: U8, data3: U8, data4: U8)
  =>
    """
    Write a byte to the buffer.
    """
    _check(4)
    _byte(data)
    _byte(data2)
    _byte(data3)
    _byte(data4)

  fun ref write_eight_bytes(data: U8, data2: U8, data3: U8, data4: U8
  , data5: U8, data6: U8, data7: U8, data8: U8) =>
    """
    Write a byte to the buffer.
    """
    _check(8)
    _byte(data)
    _byte(data2)
    _byte(data3)
    _byte(data4)
    _byte(data5)
    _byte(data6)
    _byte(data7)
    _byte(data8)

  fun ref write_sixteen_bytes(data: U8, data2: U8, data3: U8, data4: U8
  , data5: U8, data6: U8, data7: U8, data8: U8
  , data9: U8, data10: U8, data11: U8, data12: U8
  , data13: U8, data14: U8, data15: U8, data16: U8) =>
    """
    Write a byte to the buffer.
    """
    _check(16)
    _byte(data)
    _byte(data2)
    _byte(data3)
    _byte(data4)
    _byte(data5)
    _byte(data6)
    _byte(data7)
    _byte(data8)
    _byte(data9)
    _byte(data10)
    _byte(data11)
    _byte(data12)
    _byte(data13)
    _byte(data14)
    _byte(data15)
    _byte(data16)

  fun ref write_u8(data: U8) =>
    """
    Write a U8 to the buffer.
    """
    _check(1)
    _byte(data)

  fun ref write_u16(data: U16) =>
    """
    Write a U16 to the buffer.
    """
    let num_bytes = U16(0).bytewidth()
    _check(num_bytes)
    try
      _current.update_u16(_offset, data)?
      _offset = _offset + num_bytes
      _size = _size + num_bytes
    end

  fun ref write_u32(data: U32) =>
    """
    Write a U32 to the buffer.
    """
    let num_bytes = U32(0).bytewidth()
    _check(num_bytes)
    try
      _current.update_u32(_offset, data)?
      _offset = _offset + num_bytes
      _size = _size + num_bytes
    end

  fun ref write_u64(data: U64) =>
    """
    Write a U64 to the buffer.
    """
    let num_bytes = U64(0).bytewidth()
    _check(num_bytes)
    try
      _current.update_u64(_offset, data)?
      _offset = _offset + num_bytes
      _size = _size + num_bytes
    end

  fun ref write_u128(data: U128) =>
    """
    Write a U128 to the buffer.
    """
    let num_bytes = U128(0).bytewidth()
    _check(num_bytes)
    try
      _current.update_u128(_offset, data)?
      _offset = _offset + num_bytes
      _size = _size + num_bytes
    end

  // TODO: Ability to overwrite at a previous position (only if that position
  // used to be part of one of our accumulation iso's)
  fun ref write(data: ByteSeq) =>
    """
    Write a ByteSeq to the buffer.
    """
    if data.size() <= 64 then
      match data
      | let d: String => let a = d.array(); _current.copy_from(a, 0, _offset, a.size())
      | let d: Array[U8] val => _current.copy_from(d, 0, _offset, d.size())
      end
      _offset = _offset + data.size()
      _size = _size + data.size()
    else
      _append_current()
      _chunks.push(data)
      _size = _size + data.size()
    end

  fun ref writev(data: ByteSeqIter) =>
    """
    Write ByteSeqs to the buffer.
    """
    for chunk in data.values() do
      write(chunk)
    end

  fun ref done(): Array[ByteSeq] iso^ =>
    """
    Return an array of buffered ByteSeqs and reset the Writer's buffer.
    """
    _append_current()
    _size = 0
    _chunks = recover Array[ByteSeq] end

  fun ref _append_current() =>
    if _offset > 0 then
      _current.truncate(_offset)
      _offset = 0
      _chunks.push(_current = recover Array[U8] end)
    end

  fun ref _check(size': USize) =>
    if (_current.size() - _offset) < size' then
      _current.undefined(_offset + size')
    end

  fun ref _byte(data: U8) =>
    try
      _current(_offset)? = data
      _offset = _offset + 1
      _size = _size + 1
    end
