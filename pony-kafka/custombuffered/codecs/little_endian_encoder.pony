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

primitive LittleEndianEncoder
  fun u8(wb: Writer, data: U8) =>
    """
    Write a byte to the buffer.
    """
    wb.write_byte(data)

  fun i8(wb: Writer, data: I8) =>
    """
    Write an i8 to the buffer.
    """
    u8(wb, data.u8())

  fun bool(wb: Writer, data: Bool) =>
    """
    Write a Bool to the buffer.
    """
    u8(wb, data.u8())

  fun u16(wb: Writer, data: U16) =>
    """
    Write a U16 to the buffer in little-endian byte order.
    """
    wb.write_two_bytes(data.u8(), (data >> 8).u8())

  fun i16(wb: Writer, data: I16) =>
    """
    Write an I16 to the buffer in little-endian byte order.
    """
    u16(wb, data.u16())

  fun u32(wb: Writer, data: U32) =>
    """
    Write a U32 to the buffer in little-endian byte order.
    """
    wb.write_four_bytes(data.u8(), (data >> 8).u8(), (data >> 16).u8(),
      (data >> 24).u8())

  fun i32(wb: Writer, data: I32) =>
    """
    Write an I32 to the buffer in little-endian byte order.
    """
    u32(wb, data.u32())

  fun f32(wb: Writer, data: F32) =>
    """
    Write an F32 to the buffer in little-endian byte order.
    """
    u32(wb, data.bits())

  fun u64(wb: Writer, data: U64) =>
    """
    Write a U64 to the buffer in little-endian byte order.
    """
    wb.write_eight_bytes(data.u8(), (data >> 8).u8(), (data >> 16).u8(),
      (data >> 24).u8(), (data >> 32).u8(), (data >> 40).u8(),
      (data >> 48).u8(), (data >> 56).u8())

  fun i64(wb: Writer, data: I64) =>
    """
    Write an I64 to the buffer in little-endian byte order.
    """
    u64(wb, data.u64())

  fun f64(wb: Writer, data: F64) =>
    """
    Write an F64 to the buffer in little-endian byte order.
    """
    u64(wb, data.bits())

  fun u128(wb: Writer, data: U128) =>
    """
    Write a U128 to the buffer in little-endian byte order.
    """
    wb.write_sixteen_bytes(data.u8(), (data >> 8).u8(), (data >> 16).u8(),
      (data >> 24).u8(), (data >> 32).u8(), (data >> 40).u8(),
      (data >> 48).u8(), (data >> 56).u8(), (data >> 64).u8(),
      (data >> 72).u8(), (data >> 80).u8(), (data >> 88).u8(),
      (data >> 96).u8(), (data >> 104).u8(), (data >> 112).u8(),
      (data >> 120).u8())

  fun i128(wb: Writer, data: I128) =>
    """
    Write an I128 to the buffer in little-endian byte order.
    """
    u128(wb, data.u128())
