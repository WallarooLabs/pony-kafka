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

primitive LittleEndianEncoder
  fun u8(wb: Writer, data: U8) =>
    """
    Write a byte to the buffer.
    """
    wb.write_u8(data)

  fun overwrite_u8(wb: OverwriteableWriter, data: U8, pos: USize) ? =>
    """
    Overwrite a byte in the buffer.
    """
    wb.overwrite_u8(pos, data)?

  fun i8(wb: Writer, data: I8) =>
    """
    Write an i8 to the buffer.
    """
    u8(wb, data.u8())

  fun overwrite_i8(wb: OverwriteableWriter, data: I8, pos: USize) ? =>
    """
    Overwrite a byte in the buffer.
    """
    overwrite_u8(wb, data.u8(), pos)?

  fun bool(wb: Writer, data: Bool) =>
    """
    Write a Bool to the buffer.
    """
    u8(wb, BoolConverter.bool_to_u8(data))

  fun overwrite_bool(wb: OverwriteableWriter, data: Bool, pos: USize) ? =>
    """
    Overwrite a Bool in the buffer.
    """
    overwrite_u8(wb, BoolConverter.bool_to_u8(data), pos)?

  fun u16(wb: Writer, data: U16) =>
    """
    Write a U16 to the buffer in little-endian byte order.
    """
    ifdef littleendian then
      wb.write_u16(data)
    else
      wb.write_u16(data.bswap())
    end

  fun overwrite_u16(wb: OverwriteableWriter, data: U16, pos: USize) ? =>
    """
    Overwrite a U16 in the buffer.
    """
    ifdef littleendian then
      wb.overwrite_u16(pos, data)?
    else
      wb.overwrite_u16(pos, data.bswap())?
    end

  fun i16(wb: Writer, data: I16) =>
    """
    Write an I16 to the buffer in little-endian byte order.
    """
    u16(wb, data.u16())

  fun overwrite_i16(wb: OverwriteableWriter, data: I16, pos: USize) ? =>
    """
    Overwrite a I16 in the buffer.
    """
    overwrite_u16(wb, data.u16(), pos)?

  fun u32(wb: Writer, data: U32) =>
    """
    Write a U32 to the buffer in little-endian byte order.
    """
    ifdef littleendian then
      wb.write_u32(data)
    else
      wb.write_u32(data.bswap())
    end

  fun overwrite_u32(wb: OverwriteableWriter, data: U32, pos: USize) ? =>
    """
    Overwrite a U32 in the buffer.
    """
    ifdef littleendian then
      wb.overwrite_u32(pos, data)?
    else
      wb.overwrite_u32(pos, data.bswap())?
    end

  fun i32(wb: Writer, data: I32) =>
    """
    Write an I32 to the buffer in little-endian byte order.
    """
    u32(wb, data.u32())

  fun overwrite_i32(wb: OverwriteableWriter, data: I32, pos: USize) ? =>
    """
    Overwrite a I32 in the buffer.
    """
    overwrite_u32(wb, data.u32(), pos)?

  fun f32(wb: Writer, data: F32) =>
    """
    Write an F32 to the buffer in little-endian byte order.
    """
    u32(wb, data.bits())

  fun overwrite_f32(wb: OverwriteableWriter, data: F32, pos: USize) ? =>
    """
    Overwrite a F32 in the buffer.
    """
    overwrite_u32(wb, data.bits(), pos)?

  fun u64(wb: Writer, data: U64) =>
    """
    Write a U64 to the buffer in little-endian byte order.
    """
    ifdef littleendian then
      wb.write_u64(data)
    else
      wb.write_u64(data.bswap())
    end

  fun overwrite_u64(wb: OverwriteableWriter, data: U64, pos: USize) ? =>
    """
    Overwrite a U64 in the buffer.
    """
    ifdef littleendian then
      wb.overwrite_u64(pos, data)?
    else
      wb.overwrite_u64(pos, data.bswap())?
    end

  fun i64(wb: Writer, data: I64) =>
    """
    Write an I64 to the buffer in little-endian byte order.
    """
    u64(wb, data.u64())

  fun overwrite_i64(wb: OverwriteableWriter, data: I64, pos: USize) ? =>
    """
    Overwrite a I64 in the buffer.
    """
    overwrite_u64(wb, data.u64(), pos)?

  fun f64(wb: Writer, data: F64) =>
    """
    Write an F64 to the buffer in little-endian byte order.
    """
    u64(wb, data.bits())

  fun overwrite_f64(wb: OverwriteableWriter, data: F64, pos: USize) ? =>
    """
    Overwrite a F64 in the buffer.
    """
    overwrite_u64(wb, data.bits(), pos)?

  fun u128(wb: Writer, data: U128) =>
    """
    Write a U128 to the buffer in little-endian byte order.
    """
    ifdef littleendian then
      wb.write_u128(data)
    else
      wb.write_u128(data.bswap())
    end

  fun overwrite_u128(wb: OverwriteableWriter, data: U128, pos: USize) ? =>
    """
    Overwrite a U128 in the buffer.
    """
    ifdef littleendian then
      wb.overwrite_u128(pos, data)?
    else
      wb.overwrite_u128(pos, data.bswap())?
    end

  fun i128(wb: Writer, data: I128) =>
    """
    Write an I128 to the buffer in little-endian byte order.
    """
    u128(wb, data.u128())

  fun overwrite_i128(wb: OverwriteableWriter, data: I128, pos: USize) ? =>
    """
    Overwrite a I128 in the buffer.
    """
    overwrite_u128(wb, data.u128(), pos)?
