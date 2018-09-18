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

primitive BigEndianEncoder
  fun u8(wb: Writer, data: U8) =>
    """
    Write a byte to the buffer.
    """
    wb.write_u8(data)

  fun i8(wb: Writer, data: I8) =>
    """
    Write an i8 to the buffer.
    """
    u8(wb, data.u8())

  fun bool(wb: Writer, data: Bool) =>
    """
    Write a Bool to the buffer.
    """
    u8(wb, BoolConverter.bool_to_u8(data))

  fun u16(wb: Writer, data: U16) =>
    """
    Write a U16 to the buffer in big-endian byte order.
    """
    ifdef bigendian then
      wb.write_u16(data)
    else
      wb.write_u16(data.bswap())
    end

  fun i16(wb: Writer, data: I16) =>
    """
    Write an I16 to the buffer in big-endian byte order.
    """
    u16(wb, data.u16())

  fun u32(wb: Writer, data: U32) =>
    """
    Write a U32 to the buffer in big-endian byte order.
    """
    ifdef bigendian then
      wb.write_u32(data)
    else
      wb.write_u32(data.bswap())
    end

  fun i32(wb: Writer, data: I32) =>
    """
    Write an I32 to the buffer in big-endian byte order.
    """
    u32(wb, data.u32())

  fun f32(wb: Writer, data: F32) =>
    """
    Write an F32 to the buffer in big-endian byte order.
    """
    u32(wb, data.bits())

  fun u64(wb: Writer, data: U64) =>
    """
    Write a U64 to the buffer in big-endian byte order.
    """
    ifdef bigendian then
      wb.write_u64(data)
    else
      wb.write_u64(data.bswap())
    end

  fun i64(wb: Writer, data: I64) =>
    """
    Write an I64 to the buffer in big-endian byte order.
    """
    u64(wb, data.u64())

  fun f64(wb: Writer, data: F64) =>
    """
    Write an F64 to the buffer in big-endian byte order.
    """
    u64(wb, data.bits())

  fun u128(wb: Writer, data: U128) =>
    """
    Write a U128 to the buffer in big-endian byte order.
    """
    ifdef bigendian then
      wb.write_u128(data)
    else
      wb.write_u128(data.bswap())
    end

  fun i128(wb: Writer, data: I128) =>
    """
    Write an I128 to the buffer in big-endian byte order.
    """
    u128(wb, data.u128())
