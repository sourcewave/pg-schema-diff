
module Util (
  module Data.List,
  module Console,
  module Util,
  module Data.Char
)

where

import PostgreSQL
import Console
import Data.List (intercalate)
import Data.Char (isSpace)

gs :: PgMessage -> String
gs = undefined

gi :: PgMessage -> Int
gi = undefined

gb :: PgMessage -> Bool
gb = undefined


{-
gs :: SqlValue -> String
gs y@(SqlByteString x) = fromSql y
gs SqlNull = ""

gb :: SqlValue -> Bool
gb y@(SqlBool x) = fromSql y

gi :: SqlValue -> Int
gi y@(SqlInt32 x) = fromSql y
-}

data Comparison a = Equal a | LeftOnly a | RightOnly a | Unequal a a

sok :: String
sok = concat [ setColor dullGreen,  [charCheck] ,  " "]
nok :: String
nok = concat [setColor dullRed, setAttr bold, [charNotEquals], " "]

trim :: String -> String
trim [] = []
trim x@(a:y) = if (isSpace a) then trim y else x

compareIgnoringWhiteSpace :: String -> String -> Bool
compareIgnoringWhiteSpace x y = ciws (trim x) (trim y)
  where ciws x@(a:p) y@(b:q) =
             if (isSpace a && isSpace b) then ciws (trim p) (trim q) else
             if (a == b) then ciws p q else False
        ciws x [] = null (trim x)
        ciws [] y = null (trim y)

count x a = foldl (flip ((+) . fromEnum . x)) 0 a
dcount x y = foldl (\(a,b) z -> if (x z) then (a+1,b) else (a,b+1)) (0,0) y 

iseq x = case x of { Equal _ -> True; _ -> False }

class Ord a => Comparable a where
  -- doDbCompare :: [a] -> [a] -> [Comparison a]
  dbCompare :: [a] -> [a] -> [Comparison a]
  dbCompare x@(a:r) [] = map LeftOnly x
  dbCompare [] y@(a:r) = map RightOnly y
  dbCompare [] [] = []
  dbCompare x@(a:r) y@(b:s) = case compare a b of
      EQ -> objCmp a b : dbCompare r s
      LT -> LeftOnly a : dbCompare r y 
      GT -> RightOnly b : dbCompare x s

  objCmp :: a -> a -> Comparison a
