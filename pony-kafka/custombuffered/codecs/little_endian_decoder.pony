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
use "../../utils/bool_converter"

primitive LittleEndianDecoder
  fun u8(rb: Reader): U8 ? =>
    """
    Get a U8. Raise an error if there isn't enough data.
    """
    rb.read_u8()?

  fun bool(rb: Reader): Bool ? =>
    """
    Get a Bool. Raise an error if there isn't enough data.
    """
    BoolConverter.u8_to_bool(u8(rb)?)

  fun i8(rb: Reader): I8 ? =>
    """
    Get an I8.
    """
    u8(rb)?.i8()

  fun u16(rb: Reader): U16 ? =>
    """
    Get a little-endian U16.
    """
    ifdef littleendian then
      rb.read_u16()?
    else
      rb.read_u16()?.bswap()
    end

  fun i16(rb: Reader): I16 ? =>
    """
    Get a little-endian I16.
    """
    u16(rb)?.i16()

  fun u32(rb: Reader): U32 ? =>
    """
    Get a little-endian U32.
    """
    ifdef littleendian then
      rb.read_u32()?
    else
      rb.read_u32()?.bswap()
    end

  fun i32(rb: Reader): I32 ? =>
    """
    Get a little-endian I32.
    """
    u32(rb)?.i32()

  fun u64(rb: Reader): U64 ? =>
    """
    Get a little-endian U64.
    """
    ifdef littleendian then
      rb.read_u64()?
    else
      rb.read_u64()?.bswap()
    end

  fun i64(rb: Reader): I64 ? =>
    """
    Get a little-endian I64.
    """
    u64(rb)?.i64()

  fun u128(rb: Reader): U128 ? =>
    """
    Get a little-endian U128.
    """
    ifdef littleendian then
      rb.read_u128()?
    else
      rb.read_u128()?.bswap()
    end

  fun i128(rb: Reader): I128 ? =>
    """
    Get a little-endian I128.
    """
    u128(rb)?.i128()

  fun f32(rb: Reader): F32 ? =>
    """
    Get a little-endian F32.
    """
    F32.from_bits(u32(rb)?)

  fun f64(rb: Reader): F64 ? =>
    """
    Get a little-endian F64.
    """
    F64.from_bits(u64(rb)?)

  fun peek_u8(rb: PeekableReader box, offset: USize = 0): U8 ? =>
    """
    Peek at a U8 at the given offset. Raise an error if there isn't enough
    data.
    """
    rb.peek_u8(offset)?

  fun peek_i8(rb: PeekableReader box, offset: USize = 0): I8 ? =>
    """
    Peek at an I8.
    """
    peek_u8(rb, offset)?.i8()

  fun peek_u16(rb: PeekableReader box, offset: USize = 0): U16 ? =>
    """
    Peek at a little-endian U16.
    """
    ifdef littleendian then
      rb.peek_u16(offset)?
    else
      rb.peek_u16(offset)?.bswap()
    end

  fun peek_i16(rb: PeekableReader box, offset: USize = 0): I16 ? =>
    """
    Peek at a little-endian I16.
    """
    peek_u16(rb, offset)?.i16()

  fun peek_u32(rb: PeekableReader box, offset: USize = 0): U32 ? =>
    """
    Peek at a little-endian U32.
    """
    ifdef littleendian then
      rb.peek_u32(offset)?
    else
      rb.peek_u32(offset)?.bswap()
    end

  fun peek_i32(rb: PeekableReader box, offset: USize = 0): I32 ? =>
    """
    Peek at a little-endian I32.
    """
    peek_u32(rb, offset)?.i32()

  fun peek_u64(rb: PeekableReader box, offset: USize = 0): U64 ? =>
    """
    Peek at a little-endian U64.
    """
    ifdef littleendian then
      rb.peek_u64(offset)?
    else
      rb.peek_u64(offset)?.bswap()
    end

  fun peek_i64(rb: PeekableReader box, offset: USize = 0): I64 ? =>
    """
    Peek at a little-endian I64.
    """
    peek_u64(rb, offset)?.i64()

  fun peek_u128(rb: PeekableReader box, offset: USize = 0): U128 ? =>
    """
    Peek at a little-endian U128.
    """
    ifdef littleendian then
      rb.peek_u128(offset)?
    else
      rb.peek_u128(offset)?.bswap()
    end

  fun peek_i128(rb: PeekableReader box, offset: USize = 0): I128 ? =>
    """
    Peek at a little-endian I128.
    """
    peek_u128(rb, offset)?.i128()

  fun peek_f32(rb: PeekableReader box, offset: USize = 0): F32 ? =>
    """
    Peek at a little-endian F32.
    """
    F32.from_bits(peek_u32(rb, offset)?)

  fun peek_f64(rb: PeekableReader box, offset: USize = 0): F64 ? =>
    """
    Peek at a little-endian F64.
    """
    F64.from_bits(peek_u64(rb, offset)?)
