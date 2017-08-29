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

primitive VarIntEncoder
  fun u8(wb: Writer, data: U8) =>
    """
    Write a byte to the buffer in base 128 varint encoding..
    """
    _encode_varint(wb, data.u64())

  fun i8(wb: Writer, data: I8) =>
    """
    Write a i8 to the buffer in base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun bool(wb: Writer, data: Bool) =>
    """
    Write a Bool to the buffer in base 128 varint encoding.
    """
    _encode_varint(wb, data.u64())

  fun u16(wb: Writer, data: U16) =>
    """
    Write a U16 to the buffer in base 128 varint encoding.
    """
    _encode_varint(wb, data.u64())

  fun i16(wb: Writer, data: I16) =>
    """
    Write an I16 to the buffer in zig zag base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun u32(wb: Writer, data: U32) =>
    """
    Write a U32 to the buffer in base 128 varint encoding.
    """
    _encode_varint(wb, data.u64())

  fun i32(wb: Writer, data: I32) =>
    """
    Write an I32 to the buffer in zig zag base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun u64(wb: Writer, data: U64) =>
    """
    Write a U64 to the buffer in base 128 varint.
    """
    _encode_varint(wb, data.u64())

  fun i64(wb: Writer, data: I64) =>
    """
    Write an I64 to the buffer in zig zag base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun _encode_svarint(wb: Writer, data: I64) =>
    _encode_varint(wb, ((data << 1) xor (data >> 63)).u64())

  fun _encode_varint(wb: Writer, data: U64) =>
    var d = data
    repeat
      wb.write_byte((d.u8() and 0x7f) or (if (d > 0x7f) then 0x80 else 0 end))
      d = d >> 7
    until (d == 0) end

