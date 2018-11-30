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

primitive VarIntEncoder
  fun u8(wb: Writer, data: U8) =>
    """
    Write a byte to the buffer in base 128 varint encoding.
    """
    _encode_varint(wb, data.u64())

  fun overwrite_u8(wb: OverwriteableWriter, data: U8, pos: USize) ? =>
    """
    Overwrite a byte in the buffer in base 128 varint encoding.
    """
    _overwrite_encode_varint(wb, data.u64(), pos)?

  fun i8(wb: Writer, data: I8) =>
    """
    Write a i8 to the buffer in base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun overwrite_i8(wb: OverwriteableWriter, data: I8, pos: USize) ? =>
    """
    Overwrite a i8 in the buffer in base 128 varint encoding.
    """
    _overwrite_encode_svarint(wb, data.i64(), pos)?

  fun bool(wb: Writer, data: Bool) =>
    """
    Write a Bool to the buffer in base 128 varint encoding.
    """
    _encode_varint(wb, BoolConverter.bool_to_u8(data).u64())

  fun overwrite_bool(wb: OverwriteableWriter, data: Bool, pos: USize) ? =>
    """
    Overwrite a Bool in the buffer in base 128 varint encoding.
    """
    _overwrite_encode_varint(wb, BoolConverter.bool_to_u8(data).u64(), pos)?

  fun u16(wb: Writer, data: U16) =>
    """
    Write a U16 to the buffer in base 128 varint encoding.
    """
    _encode_varint(wb, data.u64())

  fun overwrite_u16(wb: OverwriteableWriter, data: U16, pos: USize) ? =>
    """
    Overwrite a U16 in the buffer in base 128 varint encoding.
    """
    _overwrite_encode_varint(wb, data.u64(), pos)?

  fun i16(wb: Writer, data: I16) =>
    """
    Write an I16 to the buffer in zig zag base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun overwrite_i16(wb: OverwriteableWriter, data: I16, pos: USize) ? =>
    """
    Overwrite a i16 in the buffer in base 128 varint encoding.
    """
    _overwrite_encode_svarint(wb, data.i64(), pos)?

  fun u32(wb: Writer, data: U32) =>
    """
    Write a U32 to the buffer in base 128 varint encoding.
    """
    _encode_varint(wb, data.u64())

  fun overwrite_u32(wb: OverwriteableWriter, data: U32, pos: USize) ? =>
    """
    Overwrite a U32 in the buffer in base 128 varint encoding.
    """
    _overwrite_encode_varint(wb, data.u64(), pos)?

  fun i32(wb: Writer, data: I32) =>
    """
    Write an I32 to the buffer in zig zag base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun overwrite_i32(wb: OverwriteableWriter, data: I32, pos: USize) ? =>
    """
    Overwrite a i32 in the buffer in base 128 varint encoding.
    """
    _overwrite_encode_svarint(wb, data.i64(), pos)?

  fun u64(wb: Writer, data: U64) =>
    """
    Write a U64 to the buffer in base 128 varint.
    """
    _encode_varint(wb, data.u64())

  fun overwrite_u64(wb: OverwriteableWriter, data: U64, pos: USize) ? =>
    """
    Overwrite a U64 in the buffer in base 128 varint encoding.
    """
    _overwrite_encode_varint(wb, data.u64(), pos)?

  fun i64(wb: Writer, data: I64) =>
    """
    Write an I64 to the buffer in zig zag base 128 varint encoding.
    """
    _encode_svarint(wb, data.i64())

  fun overwrite_i64(wb: OverwriteableWriter, data: I64, pos: USize) ? =>
    """
    Overwrite a i64 in the buffer in base 128 varint encoding.
    """
    _overwrite_encode_svarint(wb, data.i64(), pos)?

  fun _encode_svarint(wb: Writer, data: I64) =>
    _encode_varint(wb, ((data << 1) xor (data >> 63)).u64())

  fun _encode_varint(wb: Writer, data: U64) =>
    var d = data
    repeat
      let v = ((d.u8() and 0x7f) or (if (d > 0x7f) then 0x80 else 0 end))
      wb.write_u8(v)
      d = d >> 7
    until (d == 0) end

  fun _overwrite_encode_svarint(wb: OverwriteableWriter, data: I64, pos: USize) ? =>
    _overwrite_encode_varint(wb, ((data << 1) xor (data >> 63)).u64(), pos)?

  fun _overwrite_encode_varint(wb: OverwriteableWriter, data: U64, pos: USize) ? =>
    var d = data
    var o: U128 = 0
    var i: U128 = 0
    repeat
      o = (o << 8) or ((d.u8() and 0x7f) or (if (d > 0x7f) then 0x80 else 0 end)).u128()
      d = d >> 7
      i = i + 1
    until (d == 0) end
    var p = pos
    repeat
      i = i - 1
      let v = (o >> (i * 8)).u8()
      wb.overwrite_u8(p, v)?
      p = p + 1
    until (i == 0) end
