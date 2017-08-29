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

use ".."
use "itertools"

primitive LittleEndianDecoder
  fun u8(rb: Reader): U8 ? =>
    """
    Get a U8. Raise an error if there isn't enough data.
    """
    rb.read_byte()

  fun bool(rb: Reader): Bool ? =>
    """
    Get a Bool. Raise an error if there isn't enough data.
    """
    u8(rb).bool()

  fun i8(rb: Reader): I8 ? =>
    """
    Get an I8.
    """
    u8(rb).i8()

  fun u16(rb: Reader): U16 ? =>
    """
    Get a little-endian U16.
    """
    let data = rb.read_bytes(2)

    _decode_u16(data)

  fun _decode_u16(data: (Array[U8] val | Array[Array[U8] val] val)): U16 ? =>
    match data
    | let d: Array[U8] val =>
      (d(1).u16() << 8) or d(0).u16()
    | let arrays: Array[Array[U8] val] val =>
      var out: U16 = 0
      let iters = Array[Iterator[U8]]
      for a in arrays.values() do
        iters.push(a.values())
      end
      let iter_all = Iter[U8].chain(iters.values())
      var i: U16 = 0
      while iter_all.has_next() do
        out = out or (iter_all.next().u16() << (i * 8))
        i = i + 1
      end
      return out
    else
      error // should never happen
    end

  fun i16(rb: Reader): I16 ? =>
    """
    Get a little-endian I16.
    """
    u16(rb).i16()

  fun u32(rb: Reader): U32 ? =>
    """
    Get a little-endian U32.
    """
    let data = rb.read_bytes(4)

    _decode_u32(data)

  fun _decode_u32(data: (Array[U8] val | Array[Array[U8] val] val)): U32 ? =>
    match data
    | let d: Array[U8] val =>
      (d(3).u32() << 24) or (d(2).u32() << 16) or
      (d(1).u32() << 8) or d(0).u32()
    | let arrays: Array[Array[U8] val] val =>
      var out: U32 = 0
      let iters = Array[Iterator[U8]]
      for a in arrays.values() do
        iters.push(a.values())
      end
      let iter_all = Iter[U8].chain(iters.values())
      var i: U32 = 0
      while iter_all.has_next() do
        out = out or (iter_all.next().u32() << (i * 8))
        i = i + 1
      end
      return out
    else
      error // should never happen
    end

  fun i32(rb: Reader): I32 ? =>
    """
    Get a little-endian I32.
    """
    u32(rb).i32()

  fun u64(rb: Reader): U64 ? =>
    """
    Get a little-endian U64.
    """
    let data = rb.read_bytes(8)

    _decode_u64(data)

  fun _decode_u64(data: (Array[U8] val | Array[Array[U8] val] val)): U64 ? =>
    match data
    | let d: Array[U8] val =>
      (d(7).u64() << 56) or (d(6).u64() << 48) or
      (d(5).u64() << 40) or (d(4).u64() << 32) or
      (d(3).u64() << 24) or (d(2).u64() << 16) or
      (d(1).u64() << 8) or d(0).u64()
    | let arrays: Array[Array[U8] val] val =>
      var out: U64 = 0
      let iters = Array[Iterator[U8]]
      for a in arrays.values() do
        iters.push(a.values())
      end
      let iter_all = Iter[U8].chain(iters.values())
      var i: U64 = 0
      while iter_all.has_next() do
        out = out or (iter_all.next().u64() << (i * 8))
        i = i + 1
      end
      return out
    else
      error // should never happen
    end

  fun i64(rb: Reader): I64 ? =>
    """
    Get a little-endian I64.
    """
    u64(rb).i64()

  fun u128(rb: Reader): U128 ? =>
    """
    Get a little-endian U128.
    """
    let data = rb.read_bytes(16)

    _decode_u128(data)

  fun _decode_u128(data: (Array[U8] val | Array[Array[U8] val] val)): U128 ? =>
    match data
    | let d: Array[U8] val =>
      (d(15).u128() << 120) or (d(14).u128() << 112) or
      (d(13).u128() << 104) or (d(12).u128() << 96) or
      (d(11).u128() << 88) or (d(10).u128() << 80) or
      (d(9).u128() << 72) or (d(8).u128() << 64) or
      (d(7).u128() << 56) or (d(6).u128() << 48) or
      (d(5).u128() << 40) or (d(4).u128() << 32) or
      (d(3).u128() << 24) or (d(2).u128() << 16) or
      (d(1).u128() << 8) or d(0).u128()
    | let arrays: Array[Array[U8] val] val =>
      var out: U128 = 0
      let iters = Array[Iterator[U8]]
      for a in arrays.values() do
        iters.push(a.values())
      end
      let iter_all = Iter[U8].chain(iters.values())
      var i: U128 = 0
      while iter_all.has_next() do
        out = out or (iter_all.next().u128() << (i * 8))
        i = i + 1
      end
      return out
    else
      error // should never happen
    end

  fun i128(rb: Reader): I128 ? =>
    """
    Get a little-endian I128.
    """
    u128(rb).i128()

  fun f32(rb: Reader): F32 ? =>
    """
    Get a little-endian F32.
    """
    F32.from_bits(u32(rb))

  fun f64(rb: Reader): F64 ? =>
    """
    Get a little-endian F64.
    """
    F64.from_bits(u64(rb))

  fun peek_u8(rb: Reader, offset: USize = 0): U8 ? =>
    """
    Peek at a U8 at the given offset. Raise an error if there isn't enough
    data.
    """
    rb.peek_byte(offset)

  fun peek_i8(rb: Reader, offset: USize = 0): I8 ? =>
    """
    Peek at an I8.
    """
    peek_u8(rb, offset).i8()

  fun peek_u16(rb: Reader, offset: USize = 0): U16 ? =>
    """
    Peek at a little-endian U16.
    """
    let data = rb.peek_bytes(2, offset)

    _decode_u16(data)

  fun peek_i16(rb: Reader, offset: USize = 0): I16 ? =>
    """
    Peek at a little-endian I16.
    """
    peek_u16(rb, offset).i16()

  fun peek_u32(rb: Reader, offset: USize = 0): U32 ? =>
    """
    Peek at a little-endian U32.
    """
    let data = rb.peek_bytes(4, offset)

    _decode_u32(data)

  fun peek_i32(rb: Reader, offset: USize = 0): I32 ? =>
    """
    Peek at a little-endian I32.
    """
    peek_u32(rb, offset).i32()

  fun peek_u64(rb: Reader, offset: USize = 0): U64 ? =>
    """
    Peek at a little-endian U64.
    """
    let data = rb.peek_bytes(8, offset)

    _decode_u64(data)

  fun peek_i64(rb: Reader, offset: USize = 0): I64 ? =>
    """
    Peek at a little-endian I64.
    """
    peek_u64(rb, offset).i64()

  fun peek_u128(rb: Reader, offset: USize = 0): U128 ? =>
    """
    Peek at a little-endian U128.
    """
    let data = rb.peek_bytes(16, offset)

    _decode_u128(data)

  fun peek_i128(rb: Reader, offset: USize = 0): I128 ? =>
    """
    Peek at a little-endian I128.
    """
    peek_u128(rb, offset).i128()

  fun peek_f32(rb: Reader, offset: USize = 0): F32 ? =>
    """
    Peek at a little-endian F32.
    """
    F32.from_bits(peek_u32(rb, offset))

  fun peek_f64(rb: Reader, offset: USize = 0): F64 ? =>
    """
    Peek at a little-endian F64.
    """
    F64.from_bits(peek_u64(rb, offset))
