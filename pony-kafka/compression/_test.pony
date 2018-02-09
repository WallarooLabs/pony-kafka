/*

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/

use "ponytest"
use "../customlogger"

actor Main is TestList
  new create(env: Env) => PonyTest(env, this)
  new make() => None

  fun tag tests(test: PonyTest) =>
    test(_TestXXHash)
    test(_TestZlib)
    test(_TestLZ4)
    test(_TestSnappy)


class iso _TestXXHash is UnitTest
  """
  Test XXHash encoding.
  """
  fun name(): String => "compression/XXHash"

  fun apply(h: TestHelper) ? =>
    let d = [as U8: 1; 2; 6; 10; 42; 'H'; 'e'; 'l'; 'l'; 'o'; ','; ' '; 'w'
      'o'; 'r'; 'l'; 'd'; '!'; 0; 2; 56; 99]
    h.assert_eq[U32](0x02cc5d05, XXHash.hash32(Array[U8], 0)?)
    h.assert_eq[U32](0xe0fe705f, XXHash.hash32([as U8: 42], 0)?)
    h.assert_eq[U32](0x9e5e7e93, XXHash.hash32([as U8: 'H'; 'e'; 'l'; 'l'; 'o'
      ','; ' '; 'w'; 'o'; 'r'; 'l'; 'd'; '!'; 0], 0)?)
    h.assert_eq[U32](0xd6bf8459, XXHash.hash32(Array[U8], 0x42c91977)?)
    h.assert_eq[U32](0x02cc5d05, XXHash.hash32(d, 0, 4, 0)?)
    h.assert_eq[U32](0xe0fe705f, XXHash.hash32(d, 0, 4, 1)?)
    h.assert_eq[U32](0x9e5e7e93, XXHash.hash32(d, 0, 5, 14)?)

class iso _TestZlib is UnitTest
  """
  Test zlib compression/decompression.
  """
  fun name(): String => "compression/Zlib"

  fun apply(h: TestHelper) ? =>
    let b = recover Array[U8](10000) end
    b.undefined(b.space())
    let b': Array[U8] val = consume b

    let logger = StringLogger(Warn, h.env.out)

    let compressed_data = ZlibCompressor.compress(logger, b')?

    let a = ZlibDecompressor.decompress(logger, consume val compressed_data)?
    let a': Array[U8] val = consume val a

    h.assert_eq[USize](b'.size(), a'.size())

    for (i, v) in b'.pairs() do
      h.assert_eq[U8](v, a'(i)?)
    end

class iso _TestLZ4 is UnitTest
  """
  Test lz4 compression/decompression.
  """
  fun name(): String => "compression/LZ4"

  fun apply(h: TestHelper) ? =>
    let b = recover Array[U8](10000) end
    b.undefined(b.space())
    let b': Array[U8] val = consume b

    let logger = StringLogger(Warn, h.env.out)

    let compressed_data = LZ4Compressor.compress(logger, b')?

    let a = LZ4Decompressor.decompress(logger, consume val compressed_data)?
    let a': Array[U8] val = consume val a

    h.assert_eq[USize](b'.size(), a'.size())

    for (i, v) in b'.pairs() do
      h.assert_eq[U8](v, a'(i)?)
    end

class iso _TestSnappy is UnitTest
  """
  Test snappy compression/decompression.
  """
  fun name(): String => "compression/Snappy"

  fun apply(h: TestHelper) ? =>
    let b = recover Array[U8](10000) end
    b.undefined(b.space())
    let b': Array[U8] val = consume b

    let logger = StringLogger(Warn, h.env.out)

    let compressed_data = SnappyCompressor.compress(logger, b')?

    let a = SnappyDecompressor.decompress(logger, consume val compressed_data)?
    let a': Array[U8] val = consume val a

    h.assert_eq[USize](b'.size(), a'.size())

    for (i, v) in b'.pairs() do
      h.assert_eq[U8](v, a'(i)?)
    end

    let compressed_data' = SnappyCompressor.compress_java(logger, b')?

    let z = SnappyDecompressor.decompress_java(logger,
      consume val compressed_data')?
    let z': Array[U8] val = consume val z

    h.assert_eq[USize](b'.size(), z'.size())

    for (i, v) in b'.pairs() do
      h.assert_eq[U8](v, z'(i)?)
    end

