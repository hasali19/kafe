use std::io::Read;

pub fn decode_varint(mut stream: impl Read) -> eyre::Result<i64> {
    let zigzagged = leb128::read::unsigned(&mut stream)?;
    Ok(zigzag_decode(zigzagged.try_into()?))
}

fn zigzag_decode(n: u64) -> i64 {
    ((n >> 1) as i64) ^ (-((n & 1) as i64))
}
