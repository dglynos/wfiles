use xxhash_rust::xxh3::Xxh3;
use openssl::hash::{Hasher, MessageDigest};
use core::hash::Hasher as RustStdHasher; // to aleviate name mangling

pub trait ByteHasher {
    fn update(&mut self, data: &[u8]); // mixes input
    fn finish(&mut self); // finishes mixing, reseting int. state
    fn digest(&self) -> String; // outputs owned hex-encoded hash
}

const XXH3_DIGEST_SZ : usize = 8;
const MD5_DIGEST_SZ : usize = 16;
const SHA512_DIGEST_SZ : usize = 64;

pub struct QuickHasher {
    digestor : Box<Xxh3>,
    digest   : [u8; XXH3_DIGEST_SZ]
}

impl QuickHasher {
    pub fn new() -> Self {
        return Self {   digestor: Box::new(Xxh3::new()),
                     digest: [0; XXH3_DIGEST_SZ]   }
    }
}

impl ByteHasher for QuickHasher {
    fn update(&mut self, data: &[u8]) {
        self.digestor.update(data);
    }

    fn finish(&mut self) {
        let digest : u64 = self.digestor.finish();
        self.digest.copy_from_slice(&digest.to_be_bytes());
        self.digestor.reset();
    }

    fn digest(&self) -> String {
        hex::encode(self.digest)
    }
}

pub struct SlowHasher {
    digestor: Box<openssl::hash::Hasher>,
    digest: Box<[u8]>
}

impl SlowHasher {
    pub fn MD5() -> Self {
        return Self { digestor: Box::new(Hasher::new(MessageDigest::md5()).unwrap()),
                      digest: Box::new([0; MD5_DIGEST_SZ]) }
    }

    pub fn SHA512() -> Self {
        return Self { 
            digestor: Box::new(Hasher::new(MessageDigest::sha512()).unwrap()),
            digest: Box::new([0; SHA512_DIGEST_SZ]) }
    }
}

impl ByteHasher for SlowHasher {
    fn update(&mut self, data: &[u8]) {
        self.digestor.update(data).unwrap();
    }

    fn finish(&mut self) {
        let digest = self.digestor.finish().unwrap();
        self.digest.copy_from_slice(&digest);
    }

    fn digest(&self) -> String {
        hex::encode(&self.digest)
    }    
}
