// based on https://github.com/Cyan4973/xxHash/blob/dev/xxhash.c#L263

primitive XXHash
  // TODO: hash64
  // TODO: should null terminator for strings be included? to match behavior of
  // C string hashing?
  fun hash32(buffer: String, seed: U32, buffer_offset: USize, num_bytes: USize):
    U32 ?
  =>
    hash32(buffer.array(), seed, buffer_offset, num_bytes)

  fun hash32(buffer: Array[U8] box, seed: U32 = 0, buffer_offset: USize = 0,
    num_bytes: USize = -1): U32 ?
  =>
    var h32: U32 = 0
    var offset: USize = buffer_offset
    var size: USize = buffer.size().min(num_bytes)
    var last: USize = size + offset

    if size > 16 then
      let limit = last - 16
      var v1: U32 = seed + prime32_1() + prime32_2()
      var v2: U32 = seed + prime32_2()
      var v3: U32 = seed + 0
      var v4: U32 = seed - prime32_1()

      repeat
        v1 = round32(v1, read32(buffer, offset))
        offset = offset + 4
        v2 = round32(v2, read32(buffer, offset))
        offset = offset + 4
        v3 = round32(v3, read32(buffer, offset))
        offset = offset + 4
        v4 = round32(v4, read32(buffer, offset))
        offset = offset + 4
      until offset > limit end

      h32 = rotl32(v1, 1) + rotl32(v2, 7) + rotl32(v3, 12) + rotl32(v4, 18)

    else
      h32 = seed + prime32_5()
    end

    h32 = h32 + size.u32()

    while (offset + 4) <= last do
      h32 = h32 + (read32(buffer, offset) * prime32_3())
      h32 = rotl32(h32, 17) * prime32_4()
      offset = offset + 4
    end

    while offset < last do
      h32 = h32 + (buffer(offset).u32() * prime32_5())
      h32 = rotl32(h32, 11) * prime32_1()
      offset = offset + 1
    end

    h32 = h32 xor (h32 >> 15)
    h32 = h32 * prime32_2()
    h32 = h32 xor (h32 >> 13)
    h32 = h32 * prime32_3()
    h32 = h32 xor (h32 >> 16)

    h32

  fun read32(buffer: Array[U8] box, offset: USize): U32 ? =>
    // TODO: figure out some way of detecting endianness; big endian needs byte
    // swapping
    (buffer(offset + 3).u32() << 24) or (buffer(offset + 2).u32() << 16) or
    (buffer(offset + 1).u32() << 8) or buffer(offset + 0).u32()

  fun round32(seed: U32, value: U32): U32 =>
    var x = seed + (value * prime32_2())
    rotl32(x, 13)
    x * prime32_1()

  fun rotl32(x: U32, r: U32): U32 =>
    ((x << r) or (x >> (32 - r)))

  fun prime32_1(): U32 => 2654435761

  fun prime32_2(): U32 => 2246822519

  fun prime32_3(): U32 => 3266489917

  fun prime32_4(): U32 => 668265263

  fun prime32_5(): U32 => 374761393
