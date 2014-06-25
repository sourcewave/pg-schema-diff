{-# LANGUAGE BangPatterns, OverloadedStrings, FlexibleInstances, UndecidableInstances, FlexibleContexts, NoMonomorphismRestriction #-}
{-# OPTIONS_GHC -funbox-strict-fields #-}

module MD5 (md5, md5File, stringMD5, test) where

import qualified Data.ByteString as B (empty, concat, concatMap, index, singleton, length, pack, unpack, take, drop, cons, append, replicate, hGetContents)
import Data.ByteString.Internal (ByteString, toForeignPtr)
import Data.Bits ((.&.), shiftR, (.|.), complement, xor, rotateL)
import Data.List hiding ((!!))
import Data.Word (Word8, Word32, Word64)
import Foreign.Ptr (plusPtr)
import Foreign.ForeignPtr (newForeignPtr_, ForeignPtr)
import System.IO (openFile, IOMode(ReadMode))
import Data.Vector.Storable (unsafeFromForeignPtr, toList)
import System.IO.Unsafe (unsafePerformIO)
import Foreign.ForeignPtr.Unsafe (unsafeForeignPtrToPtr)
import System.Environment (getArgs)
import Debug.Trace 

blockSizeBytes :: Int
blockSizeBytes = 64 

data MD5State = MD5State Word32 Word32 Word32 Word32 deriving Show
type Digest = ByteString

md5InitialState = MD5State 0x67452301 0xEFCDAB89 0x98BADCFE 0x10325476

md5 :: ByteString -> Digest
md5 = md5Finalize . foldl' md5Update md5InitialState . blocks

md5Finalize :: MD5State -> Digest
md5Finalize (MD5State a b c d) = B.pack $ concatMap intToBytes [a,b,c,d]

intToBytes :: Word32->[Word8]
intToBytes n = map (fromIntegral . (.&. 0xff) . shiftR n) (take 4 [0,8..])

longToBytes :: Word64->ByteString
longToBytes n =  B.pack $ map (fromIntegral . (.&. 0xff) . shiftR n) (take 8 [0,8..])

blocks :: ByteString -> [ByteString]
blocks zz = blocks' zz
  where blocks' bs = if B.length bs >= blockSizeBytes then B.take blockSizeBytes bs : blocks' ( B.drop blockSizeBytes bs) 
  	                 else let lzp = (blockSizeBytes - 9) - B.length bs
  	                          lzpx = if lzp < 0 then lzp+blockSizeBytes else lzp
  	                          lm = B.concat [bs, B.singleton 0x80, B.replicate lzpx 0, longToBytes (8 * fromIntegral (B.length zz))]
  	                       in if lzp < 0 then [ B.take blockSizeBytes lm, B.drop blockSizeBytes lm] else [lm]

wordsFromBytes :: ByteString -> [Word32]
wordsFromBytes bs = toList $ unsafeFromForeignPtr (offsetForeignPtr ptr off) 0 16 where (ptr, off, _) = toForeignPtr bs

offsetForeignPtr :: ForeignPtr a -> Int -> ForeignPtr b
offsetForeignPtr fp off = unsafePerformIO $ newForeignPtr_ $ plusPtr (unsafeForeignPtrToPtr fp) off

-- takes 64 byte ByteString at a time.
md5Update :: MD5State -> ByteString -> MD5State
md5Update (MD5State a b c d) w =
    let ws = cycle (wordsFromBytes w)
 
        m1 = [ 0xd76aa478, 0xe8c7b756, 0x242070db, 0xc1bdceee, 0xf57c0faf, 0x4787c62a, 0xa8304613, 0xfd469501
             , 0x698098d8, 0x8b44f7af, 0xffff5bb1, 0x895cd7be, 0x6b901122, 0xfd987193, 0xa679438e, 0x49b40821 ]

        m2 = [ 0xf61e2562, 0xc040b340, 0x265e5a51, 0xe9b6c7aa, 0xd62f105d, 0x02441453, 0xd8a1e681, 0xe7d3fbc8
             , 0x21e1cde6, 0xc33707d6, 0xf4d50d87, 0x455a14ed, 0xa9e3e905, 0xfcefa3f8, 0x676f02d9, 0x8d2a4c8a ]

        m3 = [ 0xfffa3942, 0x8771f681, 0x6d9d6122, 0xfde5380c, 0xa4beea44, 0x4bdecfa9, 0xf6bb4b60, 0xbebfbc70
	         , 0x289b7ec6, 0xeaa127fa, 0xd4ef3085, 0x04881d05, 0xd9d4d039, 0xe6db99e5, 0x1fa27cf8, 0xc4ac5665 ]

        m4 = [ 0xf4292244, 0x432aff97, 0xab9423a7, 0xfc93a039, 0x655b59c3, 0x8f0ccc92, 0xffeff47d, 0x85845dd1
             , 0x6fa87e4f, 0xfe2ce6e0, 0xa3014314, 0x4e0811a1, 0xf7537e82, 0xbd3af235, 0x2ad7d2bb, 0xeb86d391 ]

        [z1,z2,z3,z4] = foldl ch2 [a,b,c,d] [
           (\x y z -> z `xor` (x .&. (y `xor` z)) , ws,                     [ 7, 12, 17, 22], m1),
           (\x y z -> y `xor` (z .&. (x `xor` y)) , everynth 4 (tail ws),   [ 5,  9, 14, 20], m2),
           (\x y z -> x `xor` y `xor` z ,           everynth 2 (drop 5 ws), [ 4, 11, 16, 23], m3),
           (\x y z -> y `xor` (x .|. complement z), everynth 6 ws         , [ 6, 10, 15, 21], m4) ]

    in MD5State (z1+a) (z2+b) (z3+c) (z4+d)
    where
      oo f !a_ !b_ !c_ !d_ !x s ac = b_ + rotateL (f b_ c_ d_ + (x + ac + a_)) s
      ch1 fn [a,b,c,d] (w:ws) (r1:rs) (m:ms) = let r = oo fn a b c d w r1 m
                 in r : (if null ms then [] else ch1 fn [d,r,b,c] ws rs ms )
      ch2 a (f,w,r,m) = let [h,i,j,k] = drop 12 (ch1 f a w (cycle r) m) in [h,k,j,i]
      everynth n (y:ys) = y: everynth n (drop n ys)

showHex :: Enum a => a -> ByteString
showHex = shex . fromEnum where shex x = B.cons (B.index chars a) (B.singleton (B.index chars b)) where { (a,b) = divMod x 16; chars = "0123456789abcdef" :: ByteString}

stringMD5 :: ByteString -> ByteString
stringMD5 x = B.concatMap showHex x

test :: IO ()
test = do 
     putStrLn $ "Hash is:   " ++ show (stringMD5 . md5 $ B.empty )
     putStrLn $ "Should Be: d41d8cd98f00b204e9800998ecf8427e" 

md5File :: String -> IO ()
md5File f = openFile f ReadMode >>= B.hGetContents >>= putStrLn . show. stringMD5 . md5

-- main = md5File . head =<< getArgs
