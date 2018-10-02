trait Reader
  fun size(): USize
  fun ref clear()
  fun ref skip(n: USize) ?
  fun ref append_val(data: (ByteSeq | Array[ByteSeq] val))
  fun ref append_iso(data: (ByteSeq iso | Array[ByteSeq iso] iso))
  fun ref read_u8(): U8 ?
  fun ref read_u16(): U16 ?
  fun ref read_u32(): U32 ?
  fun ref read_u64(): U64 ?
  fun ref read_u128(): U128 ?
  fun ref read_byte(): U8 ?
  fun ref read_bytes(len: USize): (Array[U8] val | Array[Array[U8] val] val | Array[U8] iso^ | Array[Array[U8] iso] iso^) ?
  fun ref read_contiguous_bytes(len: USize): (Array[U8] val | Array[U8] iso^) ?
  fun ref read_block(len: USize): Array[U8] iso^ ?
  fun ref read_until(separator: U8): Array[U8] iso^ ?
  fun ref read_line(keep_line_breaks: Bool = false): String iso^ ?

trait PeekableReader is Reader
  fun ref peek_u8(offset: USize = 0): U8 ?
  fun ref peek_u16(offset: USize = 0): U16 ?
  fun ref peek_u32(offset: USize = 0): U32 ?
  fun ref peek_u64(offset: USize = 0): U64 ?
  fun ref peek_u128(offset: USize = 0): U128 ?
  fun ref peek_byte(offset: USize = 0): U8 ?
  fun ref peek_bytes(len: USize, offset: USize = 0): (Array[U8] val | Array[Array[U8] val] val) ?
  fun ref peek_contiguous_bytes(len: USize, offset: USize = 0): (Array[U8] val | Array[U8] iso^) ?
  fun ref peek_block(len: USize, offset: USize = 0): Array[U8] iso^ ?
  fun ref peek_until(separator: U8, offset: USize = 0): Array[U8] iso^ ?
  fun ref peek_line(keep_line_breaks: Bool = false, offset: USize = 0): String iso^ ?

trait RewindableReader is PeekableReader
  fun ref set_position(pos: USize) ?
  fun current_position(): USize
  fun total_size(): USize
