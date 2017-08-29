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

primitive VarIntDecoder
  fun u8(rb: Reader): U8 ? =>
    """
    Read a byte from the buffer in base 128 varint encoding..
    """
    _decode_varint(rb, 8).u8()

  fun i8(rb: Reader): I8 ? =>
    """
    Read a byte from the buffer in base 128 varint encoding..
    """
    _decode_svarint(rb, 8).i8()

  fun bool(rb: Reader): Bool ? =>
    """
    Read a Bool from the buffer in base 128 varint encoding.
    """
    _decode_varint(rb, 8).bool()

  fun u16(rb: Reader): U16 ? =>
    """
    Read a U16 from the buffer in base 128 varint encoding.
    """
    _decode_varint(rb, 16).u16()

  fun i16(rb: Reader): I16 ? =>
    """
    Read an I16 from the buffer in zig zag base 128 varint encoding.
    """
    _decode_svarint(rb, 16).i16()

  fun u32(rb: Reader): U32 ? =>
    """
    Read a U32 from the buffer in base 128 varint encoding.
    """
    _decode_varint(rb, 32).u32()

  fun i32(rb: Reader): I32 ? =>
    """
    Read an I32 from the buffer in zig zag base 128 varint encoding.
    """
    _decode_svarint(rb, 32).i32()

  fun u64(rb: Reader): U64 ? =>
    """
    Read a U64 from the buffer in base 128 varint.
    """
    _decode_varint(rb, 64)

  fun i64(rb: Reader): I64 ? =>
    """
    Read an I64 from the buffer in zig zag base 128 varint encoding.
    """
    _decode_svarint(rb, 64)

  fun _decode_svarint(rb: Reader, bits_to_read: U64): I64 ? =>
    let d = _decode_varint(rb, bits_to_read).i64()
    (d >> 1) xor -(d and 1)

  fun _decode_varint(rb: Reader, bits_to_read: U64): U64 ? =>
    var d: U64 = 0
    var bits: U64 = 0
    var b: U64 = 0

    repeat
      if bits > bits_to_read then
        error
      end
      b = rb.read_byte().u64()
      d = d or ((b and 0x7f) << bits)
      bits = bits + 7
    until (b and 0x80) == 0 end

    d

  fun peek_u8(rb: Reader, offset: USize = 0): (U8, USize) ? =>
    """
    Read a byte from the buffer in base 128 varint encoding.
    """
    (let x, let num_bytes) = _peek_varint(rb, 8, offset)
    (x.u8(), num_bytes)

  fun peek_i8(rb: Reader, offset: USize = 0): (I8, USize) ? =>
    """
    Read a byte from the buffer in base 128 varint encoding.
    """
    (let x, let num_bytes) = _peek_svarint(rb, 8, offset)
    (x.i8(), num_bytes)

  fun peek_bool(rb: Reader, offset: USize = 0): (Bool, USize) ? =>
    """
    Read a Bool from the buffer in base 128 varint encoding.
    """
    (let x, let num_bytes) = _peek_varint(rb, 8, offset)
    (x.bool(), num_bytes)

  fun peek_u16(rb: Reader, offset: USize = 0): (U16, USize) ? =>
    """
    Read a U16 from the buffer in base 128 varint encoding.
    """
    (let x, let num_bytes) = _peek_varint(rb, 16, offset)
    (x.u16(), num_bytes)

  fun peek_i16(rb: Reader, offset: USize = 0): (I16, USize) ? =>
    """
    Read an I16 from the buffer in zig zag base 128 varint encoding.
    """
    (let x, let num_bytes) = _peek_svarint(rb, 16, offset)
    (x.i16(), num_bytes)

  fun peek_u32(rb: Reader, offset: USize = 0): (U32, USize) ? =>
    """
    Read a U32 from the buffer in base 128 varint encoding.
    """
    (let x, let num_bytes) = _peek_varint(rb, 32, offset)
    (x.u32(), num_bytes)

  fun peek_i32(rb: Reader, offset: USize = 0): (I32, USize) ? =>
    """
    Read an I32 from the buffer in zig zag base 128 varint encoding.
    """
    (let x, let num_bytes) = _peek_svarint(rb, 32, offset)
    (x.i32(), num_bytes)

  fun peek_u64(rb: Reader, offset: USize = 0): (U64, USize) ? =>
    """
    Read a U64 from the buffer in base 128 varint.
    """
    _peek_varint(rb, 64, offset)

  fun peek_i64(rb: Reader, offset: USize = 0): (I64, USize) ? =>
    """
    Read an I64 from the buffer in zig zag base 128 varint encoding.
    """
    _peek_svarint(rb, 64, offset)

  fun _peek_svarint(rb: Reader, bits_to_read: U64, offset: USize = 0):
    (I64, USize) ?
  =>
    (let d', let num_bytes) = _peek_varint(rb, bits_to_read, offset)
    let d = d'.i64()
    ((d >> 1) xor -(d and 1), num_bytes)

  fun _peek_varint(rb: Reader, bits_to_read: U64, offset: USize = 0):
    (U64, USize) ?
  =>
    var d: U64 = 0
    var bits: U64 = 0
    var b: U64 = 0
    var offset' = offset

    repeat
      if bits > bits_to_read then
        error
      end
      b = rb.peek_byte(offset').u64()
      d = d or ((b and 0x7f) << bits)
      bits = bits + 7
      offset' = offset' + 1
    until (b and 0x80) == 0 end

    (d, offset' - offset)
