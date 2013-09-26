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
import Data.Array
import Data.String
import Console

data DI = F | S | B deriving (Show, Eq)

data SingleDiff a = SingleLeft a | SingleRight a | SingleBoth a a deriving (Show, Eq)
data MultiDiff a = MultiLeft [a] | MultiRight [a] | MultiBoth [a] [a] deriving (Show, Eq)

data DL = DL {poi :: !Int, poj :: !Int, path::[DI]} deriving (Show, Eq)

instance Ord DL where x <= y = if poi x == poi y then  poj x > poj y else poi x <= poi y

canDiag :: Eq a => (a -> a -> Bool) -> [a] -> [a] -> Int -> Int -> Int -> Int -> Bool
canDiag eq as bs lena lenb = \ i j -> if i < lena && j < lenb then (arAs ! i) `eq` (arBs ! j) else False
    where arAs = listArray (0,lena - 1) as
          arBs = listArray (0,lenb - 1) bs

dstep :: (Int -> Int -> Bool) -> [DL] -> [DL]
dstep cd dls = f1 (head dls) : nextstep dls
    where f1 dl = addsnake cd $ dl {poi=poi dl + 1, path=(F : path dl)}
          f2 dl = addsnake cd $ dl {poj=poj dl + 1, path=(S : path dl)}
          nextstep (hd:tl) = let dl2 = f2 hd in if (null tl) then [dl2] else (max dl2 (f1 $ head tl)) : nextstep tl

addsnake :: (Int -> Int -> Bool) -> DL -> DL
addsnake cd dl = if (cd pi pj) then addsnake cd $ dl {poi = pi + 1, poj = pj + 1, path=(B : path dl)} else dl
    where pi = poi dl; pj = poj dl

-- | Longest Common Subsequence (and Shortest Edit Sequence )
lcs :: Eq a => (a -> a -> Bool) -> [a] -> [a] -> [DI]
lcs eq as bs = path . head . dropWhile (\dl -> poi dl /= lena || poj dl /= lenb) .
               concat . iterate (dstep cd) . (:[]) . addsnake cd $ DL {poi=0,poj=0,path=[]}
            where cd = canDiag eq as bs lena lenb
                  lena = length as; lenb = length bs

getDiffBy :: (Eq a) => (a -> a -> Bool) -> [a] -> [a] -> [SingleDiff a]
getDiffBy eq a b = markup a b . reverse $ lcs eq a b
    where markup (x:xs)   ys (F:ds) = SingleLeft x : markup xs ys ds
          markup   xs   (y:ys) (S:ds) = SingleRight y : markup xs ys ds
          markup (x:xs) (y:ys) (B:ds) = SingleBoth x y : markup xs ys ds
          markup _ _ _ = []

groupDiffs :: Eq a => [SingleDiff a] -> [MultiDiff a]
groupDiffs ds = doit ds
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
diff x y = toDiffOp $ groupDiffs $ getDiffBy (==) x y

toDiffOp :: [MultiDiff a] -> [DiffOperation a]
toDiffOp = toLineRange 1 1 
    where
       toLineRange _ _ []=[]
       toLineRange ll rl (MultiBoth ls rs:tail)= Unchanged (LineRange (LineNoPair ll (llf-1)) ls) (LineRange (LineNoPair rl (rlf-1)) rs) : toLineRange (ll+llf) (rl+rlf) tail
           where llf = ll + length ls
                 rlf = rl + length rs
       toLineRange ll rl (MultiRight lsS: MultiLeft lsF:rs)= toChange ll rl lsF lsS rs
       toLineRange ll rl (MultiLeft lsF: MultiRight lsS:rs)= toChange ll rl lsF lsS rs
       toLineRange ll rl (MultiRight lsS:rs)= Addition (LineRange (LineNoPair rl (rls-1)) lsS) (ll-1) : toLineRange ll rls rs
           where rls = rl + length lsS
       toLineRange ll rl (MultiLeft lsF:rs) = Deletion (LineRange (LineNoPair ll (llf-1)) lsF) (rl-1) : toLineRange llf rl rs
           where llf = ll + length lsF
       toChange ll rl lsF lsS rs= Change (LineRange (LineNoPair ll (llf-1)) lsF) (LineRange (LineNoPair rl (rls-1)) lsS)
               : toLineRange llf rls rs
           where rls = rl + length lsS
                 llf = ll + length lsF

prettyLines start lins = concatMap (\x -> [start,' ']++x++"\n") lins

type LineNo = Int
data LineNoPair = LineNoPair Int Int
instance Show LineNoPair where
  show (LineNoPair start end) = if start == end then (show start) else concat [show start,"," ,show end]
data LineRange a = LineRange { lrNumbers :: LineNoPair, lrContents :: [a] } deriving (Show)
data DiffOperation a = Deletion (LineRange a) LineNo 
  | Addition (LineRange a) LineNo 
  | Change (LineRange a) (LineRange a)
  | Unchanged (LineRange a) (LineRange a)

instance Show (DiffOperation String) where
  show (Deletion inLeft lineNoRight) =
    concat [show (lrNumbers inLeft), "d", show lineNoRight,"\n", prettyLines '<' (lrContents inLeft)]
  show (Addition inRight lineNoLeft) =
    concat[ show lineNoLeft, "a" , show (lrNumbers inRight), "\n", prettyLines '>' (lrContents inRight)]
  show (Change inLeft inRight) =
    concat [ show (lrNumbers inLeft), "c" , show (lrNumbers inRight), "\n",
       prettyLines '<' (lrContents inLeft), "---\n", prettyLines '>' (lrContents inRight) ]
  show (Unchanged inLeft inRight) = ""
--      concat [ show (lrNumbers inLeft), "c" , show (lrNumbers inRight), "\n",
--         prettyLines '<' (lrContents inLeft), "---\n", prettyLines '>' (lrContents inRight) ]

instance Show (DiffOperation Char) where
  show (Deletion inLeft lineNoRight) =
    concat [ {- show (lrNumbers inLeft), "d", show lineNoRight,"\n", -} peach, setExtendedBackgroundColor 15, (lrContents inLeft), treset]
  show (Addition inRight lineNoLeft) =
    concat[ {- show lineNoLeft, "a" , show (lrNumbers inRight), "\n", -} azure, setExtendedBackgroundColor 15, (lrContents inRight), treset]
  show (Change inLeft inRight) =
    concat [ {- show (lrNumbers inLeft), "c" , show (lrNumbers inRight), "\n", -}
       {- prettyLines '<' -} setExtendedBackgroundColor 15, peach, (lrContents inLeft), {-prettyLines '>' -} azure, (lrContents inRight), treset ]
  show (Unchanged inLeft inRight) = concat [ lrContents inLeft]
