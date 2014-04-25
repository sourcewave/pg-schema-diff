{-# LANGUAGE FlexibleInstances #-}

-----------------------------------------------------------------------------
-- This is an implementation of the O(ND) diff algorithm as described here
-- <http://xmailserver.org/diff2.pdf>.
-- The algorithm is the same one used by standared Unix diff.
-----------------------------------------------------------------------------

module Diff (
  diff
) where

import Prelude hiding (pi)
import Data.Array ( (!), listArray)
import Data.String (fromString)
import Console
import Data.List (isSuffixOf)
-- import Debug.Trace

data WhichInput = First | Second | Both deriving (Show, Eq)

data SingleDiff a = SingleLeft a | SingleRight a | SingleBoth a a deriving (Show, Eq)
data MultiDiff a = MultiLeft [a] | MultiRight [a] | MultiBoth [a] [a] deriving (Show, Eq)

data DL = DL {poi :: !Int, poj :: !Int, path::[WhichInput]} deriving (Show, Eq)

instance Ord DL where x <= y = if poi x == poi y then  poj x > poj y else poi x <= poi y

canDiag :: Eq a => (a -> a -> Bool) -> [a] -> [a] -> Int -> Int -> Int -> Int -> Bool
canDiag eq as bs lena lenb = \ i j -> (( i < lena && j < lenb ) && ((arAs ! i) `eq` (arBs ! j)))
    where arAs = listArray (0,lena - 1) as
          arBs = listArray (0,lenb - 1) bs

dstep :: (Int -> Int -> Bool) -> [DL] -> [DL]
dstep cd dls = f1 (head dls) : nextstep dls
    where f1 dl = addsnake cd $ dl {poi=poi dl + 1, path=First : path dl}
          f2 dl = addsnake cd $ dl {poj=poj dl + 1, path=Second : path dl}
          nextstep (hd:tl) = let dl2 = f2 hd in if null tl then [dl2] else max dl2 (f1 $ head tl) : nextstep tl

addsnake :: (Int -> Int -> Bool) -> DL -> DL
addsnake cd dl = if cd pi pj then addsnake cd $ dl {poi = pi + 1, poj = pj + 1, path=Both : path dl} else dl
    where pi = poi dl; pj = poj dl

-- | Longest Common Subsequence (and Shortest Edit Sequence )
lcs :: Eq a => (a -> a -> Bool) -> [a] -> [a] -> [WhichInput]
lcs eq as bs = path . head . dropWhile (\dl -> poi dl /= lena || poj dl /= lenb) .
               concat . iterate (dstep cd) . (:[]) . addsnake cd $ DL {poi=0,poj=0,path=[]}
            where cd = canDiag eq as bs lena lenb
                  lena = length as; lenb = length bs

getDiffBy :: (Eq a) => (a -> a -> Bool) -> [a] -> [a] -> [SingleDiff a]
getDiffBy eq a b = markup a b . reverse $ lcs eq a b
    where markup (x:xs)   ys (First:ds) = SingleLeft x : markup xs ys ds
          markup   xs   (y:ys) (Second:ds) = SingleRight y : markup xs ys ds
          markup (x:xs) (y:ys) (Both:ds) = SingleBoth x y : markup xs ys ds
          markup _ _ _ = []

groupDiffs :: Eq a => [SingleDiff a] -> [MultiDiff a]
groupDiffs = doit
    where doit (SingleLeft x : xs) = MultiLeft (x:fs) : doit rest where (fs, rest) = getLefts xs
          doit (SingleRight x : xs) = MultiRight(x:fs) : doit rest where (fs, rest) = getRights xs
          doit (SingleBoth x y : xs) = MultiBoth (x:fsx) (y:fsy) : doit rest where (fsx, fsy, rest) = getBoths xs
          doit [] = []
          getLefts (SingleLeft x : xs) = (x:fs, rest) where (fs, rest) = getLefts xs
          getLefts ls = ([], ls)
          getRights (SingleRight x : xs) = (x:fs, rest) where (fs, rest) = getRights xs
          getRights rs = ([], rs)
          getBoths (SingleBoth x y : xs) = (x:fsx, y:fsy, rest) where (fsx, fsy, rest) = getBoths xs
          getBoths bs = ([],[],bs)

diff :: Eq a => [a] -> [a] -> [DiffOperation a]
diff x y = coalesce $ toDiffOp $ groupDiffs $ getDiffBy (==) x y

toDiffOp :: Eq a => [MultiDiff a] -> [DiffOperation a]
toDiffOp = toLineRange 1 1
    where
       toLineRange _ _ []=[]
       toLineRange ls rs (MultiBoth lc rc:tail)= Unchanged ls lc rs rc : toLineRange (ls+length lc) (rs+length rc) tail
       toLineRange ls rs (MultiRight rc: MultiLeft lc:rest)= toChange ls rs lc rc rest
       toLineRange ls rs (MultiLeft lc: MultiRight rc:rest)= toChange ls rs lc rc rest
       toLineRange ls rs (MultiRight rc:rest)= Addition rs rc ls : toLineRange ls (rs + length rc) rest
       toLineRange ls rs (MultiLeft lc:rest) = Deletion ls lc rs : toLineRange (ls + length lc) rs rest
       toChange ls rs lc rc rest = Change ls lc rs rc : toLineRange (ls+length lc) (rs+length rc) rest

coalesce x = reverse . snd $ coalzip (x,[])

backspace (a, b:c) = (b : a, c)
backspace (a, []) = (a,[])

xchng a@(Change ls lc rs rc) =
  let rl = length rc
      ll = length lc
  in if rc `isSuffixOf` lc then [Deletion ls (take (length lc - rl) lc) rs 
            , Unchanged (ls+ll-rl) (drop (ll - rl) lc) rs rc]
     else [a]

coalzip :: Eq a => ([DiffOperation a],[DiffOperation a])-> ([DiffOperation a],[DiffOperation a])
-- if I stick a "crunch" in on this Change creation, it fails to do the right thing?
coalzip (a@(Deletion ls1 lc1 rs1) : b@(Unchanged ls2 lc2 rs2 rc2) : c@(Deletion ls3 lc3 rs3) : rest, z) =
  let ch = xchng $ Change ls1 (lc1++lc2++lc3) rs2 rc2 
  in coalzip (if length lc2 < 3 then backspace ( ch ++ rest, z) else (b : c : rest, a : z))
coalzip (a@(Change ls1 lc1 rs1 rc1) : b@(Unchanged ls2 lc2 rs2 rc2) : c@(Deletion ls3 lc3 rs3) : rest, z) =
  let ch = xchng $ Change ls1 (lc1++lc2++lc3) rs1 (rc1++rc2) 
  in coalzip (if length lc2 < 3 then backspace (ch ++ rest, z) else (b : c : rest, a : z))
coalzip (a@(Deletion ls1 lc1 rs1) : b@(Unchanged ls2 lc2 rs2 rc2) : c@(Change ls3 lc3 rs3 rc3) : rest, z) =
  let ch = xchng $ Change ls1 (lc1++lc2++lc3) rs2 (rc2++rc3) 
  in coalzip (if length lc2 < 3 then backspace (ch ++ rest, z) else (b : c : rest, a : z))
coalzip (a@(Change ls1 lc1 rs1 rc1) : b@(Unchanged ls2 lc2 rs2 rc2) : c@(Change ls3 lc3 rs3 rc3) : rest, z) =
  let ch = xchng $ Change ls1 (lc1++lc2++lc3) rs1 (rc1++rc2++rc3) 
  in coalzip (if length lc2 < 3 then backspace (ch ++ rest, z) else (b : c : rest, a : z))
coalzip (a@(Addition rs1 rc1 ls1) : b@(Unchanged ls2 lc2 rs2 rc2) : c@(Addition rs3 rc3 ls3) : rest, z) =
  let ch = xchng $ Change ls2 lc2 rs1 (rc1++rc2++rc3) 
  in coalzip (if length rc2 < 3 then backspace ( ch ++ rest, z) else (b : c : rest, a : z))
coalzip (a@(Change ls1 lc1 rs1 rc1) : b@(Unchanged ls2 lc2 rs2 rc2) : c@(Addition rs3 rc3 ls3) : rest, z) =
  let ch = xchng $ Change ls1 (lc1++lc2) rs1 (rc1++rc2++rc3)
  in coalzip (if length rc2 < 3 then backspace (ch ++ rest, z) else (b : c : rest, a : z))
coalzip (a@(Addition rs1 rc1 ls1) : b@(Unchanged ls2 lc2 rs2 rc2) : c@(Change ls3 lc3 rs3 rc3) : rest, z) =
  let ch = xchng $ Change ls2 (lc2++lc3) rs1 (rc1++rc2++rc3)
  in coalzip (if length rc2 < 3 then backspace (ch ++ rest, z) else (b : c : rest, a : z))
coalzip (a@(Unchanged ls1 lc1 rs1 rc1) : b@(Unchanged ls2 lc2 rs2 rc2) : rest, z) =
  coalzip $ backspace $ backspace ( Unchanged ls1 (lc1++lc2) rs1 (rc1++rc2) : rest, z)
coalzip (a:b,z) = coalzip (b, a : z)
coalzip ([],z) = ([],z)

prettyLines :: Char -> [String] -> String
prettyLines start lins = fromString $ concatMap (\x -> fromString [start,' ']++x++fromString "\n") lins

type LineNo = Int


data LineRange = LineRange Int Int
instance Show LineRange where
  show (LineRange start len) = if len == 1 then show start else concat [show start,"," ,show (start + len - 1)]

-- data LineRange a = LineRange { lrNumbers :: LineNoPair, lrContents :: [a] } deriving (Show)

data DiffOperation a = Deletion LineNo [a] LineNo 
  | Addition LineNo [a] LineNo 
  | Change LineNo [a] LineNo [a]
  | Unchanged LineNo [a] LineNo [a]

instance Show (DiffOperation String) where
  show (Deletion ls lc rs) =
    concat [show (LineRange ls (length lc)), "d", show rs,"\n", prettyLines '<' lc]
  show (Addition rs rc ls) =
    concat[ show ls, "a" , show (LineRange rs (length rc)), "\n", prettyLines '>' rc]
  show (Change ls lc rs rc) =
    concat [ show (LineRange ls (length lc) ), "c" , show (LineRange rs (length rc)), "\n",
       prettyLines '<' lc, "---\n", prettyLines '>' rc ]
  show (Unchanged ls lc rs rc) = ""
--      concat [ show (lrNumbers inLeft), "c" , show (lrNumbers inRight), "\n",
--         prettyLines '<' (lrContents inLeft), "---\n", prettyLines '>' (lrContents inRight) ]

bgColor :: Int
bgColor = 15 

instance Show (DiffOperation Char) where
  show (Deletion ls lc rs) = concat[ peach, setExtendedBackgroundColor bgColor, lc, treset]
  show (Addition rs rc ls) = concat[ azure, setExtendedBackgroundColor bgColor, rc, treset]
  show (Change ls lc rs rc) =
    concat [ setExtendedBackgroundColor bgColor, setColor dullBlack, "{", peach, lc, azure, rc, setColor dullBlack, "}", treset ]
  show (Unchanged ls lc rs rc) = lc
