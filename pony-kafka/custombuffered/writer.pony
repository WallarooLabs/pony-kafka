trait Writer
  fun ref reserve_chunks(size': USize)
  fun ref reserve_current(size': USize)
  fun size(): USize
  fun ref write_u8(data: U8)
  fun ref write_u16(data: U16)
  fun ref write_u32(data: U32)
  fun ref write_u64(data: U64)
  fun ref write_u128(data: U128)
  fun ref write(data: ByteSeq)
  fun ref writev(data: ByteSeqIter)
  fun ref done(): Array[ByteSeq] iso^

trait OverwriteableWriter is Writer
  fun ref save_overwriteable() ?
  fun ref overwrite_u8(pos: USize, data: U8) ?
  fun ref overwrite_u16(pos: USize, data: U16) ?
  fun ref overwrite_u32(pos: USize, data: U32) ?
  fun ref overwrite_u64(pos: USize, data: U64) ?
  fun ref overwrite_u128(pos: USize, data: U128) ?
