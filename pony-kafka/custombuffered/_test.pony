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

use "ponytest"
use "codecs"

actor Main is TestList
  new create(env: Env) => PonyTest(env, this)
  new make() => None

  fun tag tests(test: PonyTest) =>
    test(_TestIsoReader)
    test(_TestValReader)
    test(_TestRewindableValReader)
    test(_TestWriter)


class iso _TestIsoReader is UnitTest
  """
  Test adding to and reading from an IsoReader.
  """
  fun name(): String => "buffered/IsoReader"

  fun apply(h: TestHelper) ? =>
    let b = recover ref IsoReader end
    _ReaderTester(h, b)?

class iso _TestValReader is UnitTest
  """
  Test adding to and reading from a Reader.
  """
  fun name(): String => "buffered/ValReader"

  fun apply(h: TestHelper) ? =>
    let b = recover ref ValReader end
    _ReaderTester(h, b)?
    b.clear()
//    _RewindableReaderTester(h, b)?

class iso _TestRewindableValReader is UnitTest
  """
  Test adding to and reading from a Reader.
  """
  fun name(): String => "buffered/RewindableValReader"

  fun apply(h: TestHelper) ? =>
    let b = recover ref RewindableValReader end
    _ReaderTester(h, b)?
    b.clear()
    _RewindableReaderTester(h, b)?

class iso _TestWriter is UnitTest
  """
  Test writing to and reading from a Writer.
  """
  fun name(): String => "buffered/Writer"

  fun apply(h: TestHelper) ? =>
    let b = recover ref ValReader end
    let wb: Writer ref = Writer

    LittleEndianEncoder.u8(wb, 0x42)
    BigEndianEncoder.u16(wb, 0xDEAD)
    LittleEndianEncoder.u16(wb, 0xDEAD)
    BigEndianEncoder.u32(wb, 0xDEADBEEF)
    LittleEndianEncoder.u32(wb, 0xDEADBEEF)
    BigEndianEncoder.u64(wb, 0xDEADBEEFFEEDFACE)
    LittleEndianEncoder.u64(wb, 0xDEADBEEFFEEDFACE)
    BigEndianEncoder.u128(wb, 0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    LittleEndianEncoder.u128(wb, 0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)

    wb.write(recover [as U8: 'h'; 'i'] end)
    wb.writev(recover [as Array[U8]: [as U8: '\n'; 't'; 'h'; 'e']
                                     [as U8: 'r'; 'e'; '\r'; '\n']
                      ] end)

    VarIntEncoder.u8(wb, 0)
    VarIntEncoder.u8(wb, 0x42)
    VarIntEncoder.u16(wb, 0xDEAD)
    VarIntEncoder.u32(wb, 0xDEADBEEF)
    VarIntEncoder.u64(wb, 0xDEADBEEFFEEDFACE)
    VarIntEncoder.i8(wb, -42)
    VarIntEncoder.i16(wb, -0xEAD)
    VarIntEncoder.i32(wb, -0xEADBEEF)
    VarIntEncoder.i64(wb, -0xEADBEEFFEEDFACE)

    VarIntEncoder.i64(wb, 150)
    VarIntEncoder.i64(wb, -150)
    VarIntEncoder.i64(wb, -2147483648)

    for bs in wb.done().values() do
      try
        b.append(bs as Array[U8] val)
      end
    end

    // These expectations peek into the buffer without consuming bytes.
    h.assert_eq[U8](BigEndianDecoder.peek_u8(b)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.peek_u16(b, 1)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.peek_u16(b, 3)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.peek_u32(b, 5)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.peek_u32(b, 9)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.peek_u64(b, 13)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.peek_u64(b, 21)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.peek_u128(b, 29)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.peek_u128(b, 45)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)

    h.assert_eq[U8](BigEndianDecoder.peek_u8(b, 61)?, 'h')
    h.assert_eq[U8](BigEndianDecoder.peek_u8(b, 62)?, 'i')

    var offset: USize = 0
    var pos: USize = 71

    (let x, offset) = VarIntDecoder.peek_u8(b, pos)?
    h.assert_eq[U8](x, 0)
    pos = pos + offset

    (let x', offset) = VarIntDecoder.peek_u8(b, pos)?
    h.assert_eq[U8](x', 0x42)
    pos = pos + offset

    (let y, offset) = VarIntDecoder.peek_u16(b, pos)?
    h.assert_eq[U16](y, 0xDEAD)
    pos = pos + offset

    (let y', offset) = VarIntDecoder.peek_u32(b, pos)?
    h.assert_eq[U32](y', 0xDEADBEEF)
    pos = pos + offset

    (let y'', offset) = VarIntDecoder.peek_u64(b, pos)?
    h.assert_eq[U64](y'', 0xDEADBEEFFEEDFACE)
    pos = pos + offset


    (let z, offset) = VarIntDecoder.peek_i8(b, pos)?
    h.assert_eq[I8](z, -42)
    pos = pos + offset

    (let z', offset) = VarIntDecoder.peek_i16(b, pos)?
    h.assert_eq[I16](z', -0xEAD)
    pos = pos + offset

    (let z'', offset) = VarIntDecoder.peek_i32(b, pos)?
    h.assert_eq[I32](z'', -0xEADBEEF)
    pos = pos + offset

    (let z''', offset) = VarIntDecoder.peek_i64(b, pos)?
    h.assert_eq[I64](z''', -0xEADBEEFFEEDFACE)


    // These expectations consume bytes from the head of the buffer.
    h.assert_eq[U8](BigEndianDecoder.u8(b)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)

    h.assert_eq[String](b.line()?, "hi")
    h.assert_eq[String](b.line()?, "there")

    h.assert_eq[U8](VarIntDecoder.u8(b)?, 0)
    h.assert_eq[U8](VarIntDecoder.u8(b)?, 0x42)
    h.assert_eq[U16](VarIntDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U32](VarIntDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U64](VarIntDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[I8](VarIntDecoder.i8(b)?, -42)
    h.assert_eq[I16](VarIntDecoder.i16(b)?, -0xEAD)
    h.assert_eq[I32](VarIntDecoder.i32(b)?, -0xEADBEEF)
    h.assert_eq[I64](VarIntDecoder.i64(b)?, -0xEADBEEFFEEDFACE)

    h.assert_eq[U64](VarIntDecoder.u64(b)?, 300)
    h.assert_eq[U64](VarIntDecoder.u64(b)?, 299)
    h.assert_eq[U64](VarIntDecoder.u64(b)?, 4294967295)

    b.append(recover [as U8: 'h'; 'i'] end)

    try
      b.line()?
      h.fail("shouldn't have a line")
    end

    b.append(recover [as U8: '!'; '\n'] end)
    h.assert_eq[String](b.line()?, "hi!")

primitive _ReaderTester
  """
  Test adding to and reading from a Reader.
  """
  new create(h: TestHelper, b: Reader) ? =>
    // The initial bytes are all spread across multiple arrays to
    // test `else` condition when all data isn't in a single array
    // when numeric types are being read
    b.append_iso(recover [as U8: 0x42] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xDE] end)

    // normal/contiguous data
    b.append_iso(recover [as U8:
      0x42
      0xDE; 0xAD
      0xAD; 0xDE
      0xDE; 0xAD; 0xBE; 0xEF
      0xEF; 0xBE; 0xAD; 0xDE
      0xDE; 0xAD; 0xBE; 0xEF; 0xFE; 0xED; 0xFA; 0xCE
      0xCE; 0xFA; 0xED; 0xFE; 0xEF; 0xBE; 0xAD; 0xDE
      ] end)

    b.append_iso(recover [as U8:
      0xDE; 0xAD; 0xBE; 0xEF; 0xFE; 0xED; 0xFA; 0xCE
      0xDE; 0xAD; 0xBE; 0xEF; 0xFE; 0xED; 0xFA; 0xCE
      0xCE; 0xFA; 0xED; 0xFE; 0xEF; 0xBE; 0xAD; 0xDE
      0xCE; 0xFA; 0xED; 0xFE; 0xEF; 0xBE; 0xAD; 0xDE
      ] end)

    b.append_iso(recover [as U8: 'h'; 'i'] end)
    b.append_iso(recover [as U8: '\n'; 't'; 'h'; 'e'] end)
    b.append_iso(recover [as U8: 'r'; 'e'; '\r'; '\n'] end)

    // These expectations consume bytes from the head of the buffer.
    // The initial bytes are all spread across multiple arrays to
    // test `else` condition when all data isn't in a single array
    // when numeric types are being read
    h.assert_eq[U8](LittleEndianDecoder.u8(b)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)


    // These expectations consume bytes from the head of the buffer.
    h.assert_eq[U8](LittleEndianDecoder.u8(b)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)


    h.assert_eq[String](String.from_array(b.read_contiguous_bytes(2)?), "hi")

    h.assert_eq[String](String.from_array(b.read_contiguous_bytes(4)?), "\nthe")

    h.assert_eq[String](String.from_array(b.read_contiguous_bytes(4)?), "re\r\n")

    h.assert_eq[USize](b.size(), 0)

    b.append_iso(recover [as U8: 'h'; 'i'] end)
    b.append_iso(recover [as U8: '\n'; 't'; 'h'; 'e'] end)
    b.append_iso(recover [as U8: 'r'; 'e'; '\r'; '\n'] end)

    b.append_iso(recover [as U8: 'h'; 'i'] end)
    b.append_iso(recover [as U8: '\n'; 't'; 'h'; 'e'] end)
    b.append_iso(recover [as U8: 'r'; 'e'; '\r'; '\n'] end)

    b.append_iso(recover [as U8: 0] end)
    b.append_iso(recover [as U8: 172; 2] end)

    h.assert_eq[String](String.from_array(b.block(2)?), "hi")
    h.assert_eq[String](String.from_array(b.block(8)?), "\nthere\r\n")

    b.skip(10)?

    h.assert_eq[U8](VarIntDecoder.u8(b)?, 0)
    h.assert_eq[U32](VarIntDecoder.u32(b)?, 300)

    b.append_iso(recover [as U8: 's'; 't'; 'r'; '1'] end)
    try
      b.read_until(0)?
      h.fail("should fail reading until 0")
    end
    b.append_iso(recover [as U8: 0] end)
    b.append_iso(recover [as U8: 'f'; 'i'; 'e'; 'l'; 'd'; '1'; ';'
      'f'; 'i'; 'e'; 'l'; 'd'; '2'; ';'; ';'] end)
    h.assert_eq[String](String.from_array(b.read_until(0)?), "str1")
    h.assert_eq[String](String.from_array(b.read_until(';')?), "field1")
    h.assert_eq[String](String.from_array(b.read_until(';')?), "field2")
    // read an empty field
    h.assert_eq[String](String.from_array(b.read_until(';')?), "")

    b.append_iso(recover [as U8: 'h'; 'i'] end)

    h.assert_eq[USize](b.size(), 2)
    try
      h.assert_eq[String](b.line()?, "")
      h.fail("shouldn't have a line")
    end

    h.assert_eq[U8](b.read_u8()?, 'h')
    h.assert_eq[U8](b.read_u8()?, 'i')

    b.append_iso(recover [as U8: '!'; '\n'] end)
    h.assert_eq[String](b.line()?, "!")

    // the last byte is consumed by the reader
    h.assert_eq[USize](b.size(), 0)

    b.append_iso(recover [as U8: 'h'; 'i'] end)
    b.append_iso(recover [as U8: '\n'; 't'; 'h'; 'e'] end)
    b.append_iso(recover [as U8: 'r'; 'e'; '\r'; '\n'] end)

    b.clear()

    h.assert_eq[USize](b.size(), 0)

primitive _RewindableReaderTester
  """
  Test adding to and reading from a Reader.
  """
  new create(h: TestHelper, b: RewindableReader) ? =>
    // The initial bytes are all spread across multiple arrays to
    // test `else` condition when all data isn't in a single array
    // when numeric types are being read
    b.append_iso(recover [as U8: 0x42] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xDE] end)
    b.append_iso(recover [as U8: 0xCE] end)
    b.append_iso(recover [as U8: 0xFA] end)
    b.append_iso(recover [as U8: 0xED] end)
    b.append_iso(recover [as U8: 0xFE] end)
    b.append_iso(recover [as U8: 0xEF] end)
    b.append_iso(recover [as U8: 0xBE] end)
    b.append_iso(recover [as U8: 0xAD] end)
    b.append_iso(recover [as U8: 0xDE] end)

    // normal/contiguous data
    b.append_iso(recover [as U8:
      0x42
      0xDE; 0xAD
      0xAD; 0xDE
      0xDE; 0xAD; 0xBE; 0xEF
      0xEF; 0xBE; 0xAD; 0xDE
      0xDE; 0xAD; 0xBE; 0xEF; 0xFE; 0xED; 0xFA; 0xCE
      0xCE; 0xFA; 0xED; 0xFE; 0xEF; 0xBE; 0xAD; 0xDE
      0xDE; 0xAD; 0xBE; 0xEF; 0xFE; 0xED; 0xFA; 0xCE
      0xDE; 0xAD; 0xBE; 0xEF; 0xFE; 0xED; 0xFA; 0xCE
      0xCE; 0xFA; 0xED; 0xFE; 0xEF; 0xBE; 0xAD; 0xDE
      0xCE; 0xFA; 0xED; 0xFE; 0xEF; 0xBE; 0xAD; 0xDE
      ] end)

    b.append_iso(recover [as U8: 'h'; 'i'] end)
    b.append_iso(recover [as U8: '\n'; 't'; 'h'; 'e'] end)
    b.append_iso(recover [as U8: 'r'; 'e'; '\r'; '\n'] end)

    // These expectations peek into the buffer without consuming bytes.
    // The initial bytes are all spread across multiple arrays to
    // test `else` condition when all data isn't in a single array
    // when numeric types are being read
    h.assert_eq[U8](LittleEndianDecoder.peek_u8(b)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.peek_u16(b, 1)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.peek_u16(b, 3)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.peek_u32(b, 5)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.peek_u32(b, 9)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.peek_u64(b, 13)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.peek_u64(b, 21)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.peek_u128(b, 29)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.peek_u128(b, 45)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)


    // These expectations peek into the buffer without consuming bytes.
    h.assert_eq[U8](LittleEndianDecoder.peek_u8(b, 61)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.peek_u16(b, 62)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.peek_u16(b, 64)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.peek_u32(b, 66)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.peek_u32(b, 70)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.peek_u64(b, 74)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.peek_u64(b, 82)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.peek_u128(b, 90)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.peek_u128(b, 106)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)


    h.assert_eq[U8](LittleEndianDecoder.peek_u8(b, 122)?, 'h')
    h.assert_eq[U8](LittleEndianDecoder.peek_u8(b, 123)?, 'i')


    // These expectations consume bytes from the head of the buffer.
    // The initial bytes are all spread across multiple arrays to
    // test `else` condition when all data isn't in a single array
    // when numeric types are being read
    h.assert_eq[U8](LittleEndianDecoder.u8(b)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)


    // These expectations consume bytes from the head of the buffer.
    h.assert_eq[U8](LittleEndianDecoder.u8(b)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)

    h.assert_eq[USize](b.current_position(), 122)
    h.assert_eq[String](b.line()?, "hi")
    h.assert_eq[USize](b.current_position(), 125)
    try
      b.read_until(0)?
      h.fail("should fail reading until 0")
    end
    h.assert_eq[String](b.line()?, "there")
    h.assert_eq[USize](b.current_position(), 132)
    h.assert_eq[USize](b.total_size(), 132)
    h.assert_eq[USize](b.size(), 0)

    b.append_iso(recover [as U8: 'h'; 'i'] end)

    h.assert_eq[USize](b.current_position(), 132)
    h.assert_eq[USize](b.total_size(), 134)
    h.assert_eq[USize](b.size(), 2)
    try
      h.assert_eq[String](b.line()?, "")
      h.fail("shouldn't have a line")
    end
    h.assert_eq[USize](b.current_position(), 132)

    h.assert_eq[U8](b.read_u8()?, 'h')
    h.assert_eq[U8](b.read_u8()?, 'i')

    b.append_iso(recover [as U8: '!'; '\n'] end)
    h.assert_eq[String](b.line()?, "!")

    b.append_iso(recover [as U8: 's'; 't'; 'r'; '1'] end)
    try
      b.read_until(0)?
      h.fail("should fail reading until 0")
    end
    b.append_iso(recover [as U8: 0] end)
    b.append_iso(recover [as U8: 'f'; 'i'; 'e'; 'l'; 'd'; '1'; ';'
      'f'; 'i'; 'e'; 'l'; 'd'; '2'; ';'; ';'] end)
    h.assert_eq[String](String.from_array(b.read_until(0)?), "str1")
    h.assert_eq[String](String.from_array(b.read_until(';')?), "field1")
    h.assert_eq[String](String.from_array(b.read_until(';')?), "field2")
    // read an empty field
    h.assert_eq[String](String.from_array(b.read_until(';')?), "")

    b.append_iso(recover [as U8: 0] end)
    b.append_iso(recover [as U8: 172; 2] end)


    h.assert_eq[U8](VarIntDecoder.u8(b)?, 0)
    h.assert_eq[U32](VarIntDecoder.u32(b)?, 300)

    // the last byte is consumed by the reader
    h.assert_eq[USize](b.size(), 0)

    // rewind to beginning of buffer
    b.set_position(0)?

    // These expectations peek into the buffer without consuming bytes.
    // The initial bytes are all spread across multiple arrays to
    // test `else` condition when all data isn't in a single array
    // when numeric types are being read
    h.assert_eq[U8](LittleEndianDecoder.peek_u8(b)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.peek_u16(b, 1)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.peek_u16(b, 3)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.peek_u32(b, 5)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.peek_u32(b, 9)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.peek_u64(b, 13)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.peek_u64(b, 21)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.peek_u128(b, 29)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.peek_u128(b, 45)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)


    // These expectations peek into the buffer without consuming bytes.
    h.assert_eq[U8](LittleEndianDecoder.peek_u8(b, 61)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.peek_u16(b, 62)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.peek_u16(b, 64)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.peek_u32(b, 66)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.peek_u32(b, 70)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.peek_u64(b, 74)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.peek_u64(b, 82)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.peek_u128(b, 90)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.peek_u128(b, 106)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)


    h.assert_eq[U8](LittleEndianDecoder.peek_u8(b, 122)?, 'h')
    h.assert_eq[U8](LittleEndianDecoder.peek_u8(b, 123)?, 'i')


    // These expectations consume bytes from the head of the buffer.
    // The initial bytes are all spread across multiple arrays to
    // test `else` condition when all data isn't in a single array
    // when numeric types are being read
    h.assert_eq[U8](LittleEndianDecoder.u8(b)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)


    // These expectations consume bytes from the head of the buffer.
    h.assert_eq[U8](LittleEndianDecoder.u8(b)?, 0x42)
    h.assert_eq[U16](BigEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U16](LittleEndianDecoder.u16(b)?, 0xDEAD)
    h.assert_eq[U32](BigEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U32](LittleEndianDecoder.u32(b)?, 0xDEADBEEF)
    h.assert_eq[U64](BigEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U64](LittleEndianDecoder.u64(b)?, 0xDEADBEEFFEEDFACE)
    h.assert_eq[U128](BigEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)
    h.assert_eq[U128](LittleEndianDecoder.u128(b)?,
      0xDEADBEEFFEEDFACEDEADBEEFFEEDFACE)

    h.assert_eq[USize](b.current_position(), 122)
    h.assert_eq[String](b.line()?, "hi")
    h.assert_eq[USize](b.current_position(), 125)
    h.assert_eq[String](String.from_array(b.read_contiguous_bytes(7)?), "there\r\n")
    h.assert_eq[USize](b.current_position(), 132)
    h.assert_eq[USize](b.total_size(), 159)
    h.assert_eq[USize](b.size(), 27)

    h.assert_eq[USize](b.current_position(), 132)
    h.assert_eq[USize](b.total_size(), 159)
    h.assert_eq[USize](b.size(), 27)
    h.assert_eq[String](b.line()?, "hi!")

    h.assert_eq[String](String.from_array(b.read_until(0)?), "str1")
    h.assert_eq[String](String.from_array(b.peek_contiguous_bytes(10)?), "field1;fie")
    h.assert_eq[String](String.from_array(b.read_until(';')?), "field1")
    h.assert_eq[String](String.from_array(b.read_until(';')?), "field2")
    // read an empty field
    h.assert_eq[String](String.from_array(b.read_until(';')?), "")


    h.assert_eq[U8](VarIntDecoder.u8(b)?, 0)
    h.assert_eq[U32](VarIntDecoder.u32(b)?, 300)

    // the last byte is consumed by the reader
    h.assert_eq[USize](b.size(), 0)
