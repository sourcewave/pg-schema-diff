{-# LANGUAGE FlexibleInstances #-}

module Acl (
  cvtacl,
  showAclDiffs,
  Acl
)
where

import Util
import Data.List
import Debug.Trace

cvtacl :: String -> [Acl]
cvtacl x = if (null x) then [] else sort $ map toAcl ((read ("[" ++ (tail . init) x ++ "]")) :: [String])

instance Comparable Acl where
  objCmp a b =
    if (grantee a == grantee b && privileges a == privileges b) then Equal a 
    else Unequal a b

data Acl = Acl { grantee :: String, privileges :: [Char], grantor :: String }

instance Show Acl where
  show a = concat [grantee a, " has ", intercalate "," $ map privName (privileges a) ]

privName x = case x of { ('a') -> "INSERT"; ('r') -> "SELECT"; ('w')->"UPDATE"; ('d')->"DELETE"
                        ; ('D') -> "TRUNCATE"; ('x') -> "REFERENCES"; ('t') -> "TRIGGER"; ('X')->"EXECUTE"
                        ; ('U') -> "USAGE"; ('C') -> "CREATE"; ('T')->"CREATE TEMP"; ('c')->"CONNECT"; otherwise ->"? "++[x] }

toAcl x = let  (p,q) = (break ('/'==) x)
               (a,b) = (break ('='==) p)
          in Acl (if (null a) then "public" else if (head a == '"') then (read a :: String) else a) (tail b) (tail q)

instance Ord Acl where
  compare a b = compare (grantee a) (grantee b)

instance Eq Acl where
  (==) a b = grantee a == grantee b && privileges a == privileges b
  
instance Show (Comparison Acl) where
  show (Equal x) = ""
  show (LeftOnly a) = concat [azure, [charLeftArrow]," ", show a, treset]
  show (RightOnly a) = concat [peach, [charRightArrow], " ", show a,  treset]
  show (Unequal a b) = let p = map privName (privileges a \\ privileges b)
                           q = map privName (privileges b \\ privileges a)
                       in concat [nok, grantee a, if (null p) then "" else concat [" also has ", azure , intercalate "," p],
                                             if (null q) then "" else concat ["; lacks ", peach, intercalate "," q],
                                  treset ]

showAclDiffs a b = 
  let dc = dbCompare a b
      ddc = filter (\l -> case l of { Equal _ -> False; otherwise -> True }) dc
  in if ( a /=  b) 
     then concat [ setAttr bold, "\n acls: ", treset, "\n  ", intercalate "\n  " $ map show ddc] 
     else ""
