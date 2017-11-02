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


  // cant handle iso strings until `iso_array()` is added to string
//  fun ref append(data: ByteSeq iso) =>
  fun ref append(data: Array[U8] iso) =>
    """
    Add a chunk of data.
    """
    let data_array =
      match consume data
      | let data': Array[U8] iso => consume data'
//      | let data': String iso => (consume data').array()
      end

    _available = _available + data_array.size()
    _chunks.push(consume data_array)

/*
  fun ref append(data: Array[ByteSeq iso] iso) =>
    """
    Add a chunk of data.
    """
    for d in data.values() do
      match d
      | let s: String => append(s.array())
      | let a: Array[U8] val => append(a)
      end
    end
*/

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
      var out = arr.shift()?
      var i = out.size()
      out.undefined(num_bytes)
      while arr.size() > 0 do
        let a = recover val arr.shift()? end
        out = recover
          let r = consume ref out
          a.copy_to(r, 0, i, a.size())
          i = i + a.size()
          consume r
        end
      end
      out
    end

  fun ref read_byte(): U8 ? =>
    """
    Get a single byte.
    """
    if _available < 1 then
      error
    end

    _available = _available - 1
    let r = _chunks(0)?(0)?
    if _chunks(0)?.size() > 1 then
      _chunks(0)?.trim_in_place(1)
    else
      _chunks.shift()?
    end
    r

  fun ref read_bytes(len: USize): (Array[U8] iso^ | Array[Array[U8] iso] iso^) ?
  =>
    _read_bytes(len)?._2

  fun ref _read_bytes(len: USize): (USize, (Array[U8] iso^ | Array[Array[U8] iso] iso^)) ?
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
    var out = recover Array[Array[U8] iso] end
    var i = USize(0)

    while i < len do
      var data = _chunks.shift()?
      let avail = data.size()
      let need = len - i
      let copy_len = need.min(avail)

      (let next_segment, data) = (consume data).chop(need)

      if avail > need then
        _chunks.unshift(consume data)
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

    var out = recover Array[Array[U8] iso] end

    let avail = _chunks(0)?.size()
    if avail < len then
      error
    end

    var data = _chunks.shift()?
    let need = len
    let copy_len = need.min(avail)

    (let next_segment, data) = (consume data).chop(need)
    if data.size() > 0 then
      _chunks.unshift(consume data)
    end
    _available = _available - len
    next_segment
