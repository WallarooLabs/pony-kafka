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

class IsoReader is Reader
  """
  Store network data and provide a parsing interface.
  """
  embed _chunks: Array[Array[U8] iso] = _chunks.create()
  var _available: USize = 0

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


  fun ref _append(data: ByteSeq iso) =>
    """
    Add a chunk of data.
    """
    let data_array =
      match consume data
      | let data': Array[U8] iso => consume data'
      | let data': String iso => (consume data').iso_array()
      end

    _available = _available + data_array.size()
    _chunks.push(consume data_array)

  fun ref append(data: (ByteSeq iso | Array[ByteSeq iso] iso)) =>
    """
    Add a chunk of data.
    """
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
      _available = _available - n
      var rem = n

      while rem > 0 do
        let avail = _chunks(0)?.size()

        if avail > rem then
          _chunks(0)?.trim_in_place(rem)
          break
        end

        rem = rem - avail
      end

    else
      error
    end


  fun ref block(len: USize): Array[U8] iso^ ? =>
    """
    Return a block as a contiguous chunk of memory.
    """
    (let num_bytes, let data) = _read_bytes(len)?

    match consume data
    | let a: Array[U8] iso =>
      a
    | let arr: Array[Array[U8] iso] iso =>
      // reverse in place to avoid having to do expensive shifts for each element
      arr.reverse_in_place()
      var out = arr.pop()?
      var i = out.size()
      out.undefined(num_bytes)
      while arr.size() > 0 do
        let a = recover val arr.pop()? end
        out = recover
          let r = consume ref out
          a.copy_to(r, 0, i, a.size())
          i = i + a.size()
          consume r
        end
      end
      out
    end

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
      let r = _chunks(0)?.read_u16(0)?
      if _chunks(0)?.size() > num_bytes then
        _chunks(0)?.trim_in_place(num_bytes)
      else
        _chunks.shift()?
      end
      _available = _available - num_bytes
      r
    else
      let data = read_bytes(num_bytes)?

      _decode_u16(consume data)?
    end

  fun _decode_u16(data: (Array[U8] val | Array[Array[U8] iso] val)): U16 ? =>
    match data
    | let d: Array[U8] val =>
      d.read_u16(0)?
    | let d: (Array[Array[U8] iso] val) =>
      _decode_u16_array(d)?
    end

  fun _decode_u16_array(data: Array[Array[U8] iso] val): U16 ? =>
    var out: U16 = 0
    let iters = Array[Iterator[U8]]
    for a in data.values() do
      iters.push(a.values())
    end
    let iter_all = Iter[U8].chain(iters.values())
    var i: U16 = 0
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
      let r = _chunks(0)?.read_u32(0)?
      if _chunks(0)?.size() > num_bytes then
        _chunks(0)?.trim_in_place(num_bytes)
      else
        _chunks.shift()?
      end
      _available = _available - num_bytes
      r
    else
      let data = read_bytes(num_bytes)?

      _decode_u32(consume data)?
    end

  fun _decode_u32(data: (Array[U8] val | Array[Array[U8] iso] val)): U32 ? =>
    match data
    | let d: Array[U8] val =>
      d.read_u32(0)?
    | let d: (Array[Array[U8] iso] val) =>
      _decode_u32_array(d)?
    end

  fun _decode_u32_array(data: Array[Array[U8] iso] val): U32 ? =>
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
      let r = _chunks(0)?.read_u64(0)?
      if _chunks(0)?.size() > num_bytes then
        _chunks(0)?.trim_in_place(num_bytes)
      else
        _chunks.shift()?
      end
      _available = _available - num_bytes
      r
    else
      let data = read_bytes(num_bytes)?

      _decode_u64(consume data)?
    end

  fun _decode_u64(data: (Array[U8] val | Array[Array[U8] iso] val)): U64 ? =>
    match data
    | let d: Array[U8] val =>
      d.read_u64(0)?
    | let d: (Array[Array[U8] iso] val) =>
      _decode_u64_array(d)?
    end

  fun _decode_u64_array(data: Array[Array[U8] iso] val): U64 ? =>
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
      let r = _chunks(0)?.read_u128(0)?
      if _chunks(0)?.size() > num_bytes then
        _chunks(0)?.trim_in_place(num_bytes)
      else
        _chunks.shift()?
      end
      _available = _available - num_bytes
      r
    else
      let data = read_bytes(num_bytes)?

      _decode_u128(consume data)?
    end

  fun _decode_u128(data: (Array[U8] val | Array[Array[U8] iso] val)): U128 ? =>
    match data
    | let d: Array[U8] val =>
      d.read_u128(0)?
    | let d: (Array[Array[U8] iso] val) =>
      _decode_u128_array(d)?
    end

  fun _decode_u128_array(data: Array[Array[U8] iso] val): U128 ? =>
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
    let r = _chunks(0)?.read_u8(0)?
    _available = _available - num_bytes
    if _chunks(0)?.size() > num_bytes then
      _chunks(0)?.trim_in_place(num_bytes)
    else
      _chunks.shift()?
    end
    r

  fun ref read_bytes(len: USize): (Array[U8] iso^ | Array[Array[U8] iso] iso^) ?
  =>
    _read_bytes(len)?._2

  fun ref _read_bytes(len: USize):
    (USize, (Array[U8] iso^ | Array[Array[U8] iso] iso^)) ?
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

    // TODO: rewrite to avoid allocation of out array if all data if in first
    // chunk
    _available = _available - len
    var out = recover Array[Array[U8] iso] end
    var i = USize(0)

    while i < len do
      var data = _chunks.shift()?
      let avail = data.size()
      let need = len - i
      let copy_len = need.min(avail)

      (let next_segment, data) = (consume data).chop(need)

      if avail >= need then
        if data.size() > 0 then
          _chunks.unshift(consume data)
        end

        if out.size() == 0 then
          return (copy_len, consume next_segment)
        else
          out.push(consume next_segment)
          break
        end
      else
        out.push(consume next_segment)
      end

      i = i + copy_len
    end

    (i, consume out)

  fun ref read_contiguous_bytes(len: USize): Array[U8] iso^ ? =>
    """
    Return a block as a contiguous chunk of memory without copying if possible
    or throw an error.
    """
    if len == 0 then
      return recover Array[U8] end
    end

    if _available < len then
      error
    end

    let avail = _chunks(0)?.size()

    // if we have enough data but not in a single contiguous chunk, call `block`
    // to copy chunks together
    if avail < len then
      return block(len)?
    end

    var out = recover Array[Array[U8] iso] end

    var data = _chunks.shift()?
    let need = len
    let copy_len = need.min(avail)

    (let next_segment, data) = (consume data).chop(need)
    if data.size() > 0 then
      _chunks.unshift(consume data)
    end
    _available = _available - len
    next_segment
