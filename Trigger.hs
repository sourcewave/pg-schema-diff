{-# LANGUAGE QuasiQuotes, FlexibleInstances #-}

module Trigger where

import Str(str)
import Acl
import Util
import Console
import Diff

triggers = [str|
SELECT n.nspname as "Schema", c.relname AS "Relation", t.tgname AS "Name", tgtype AS "Type", t.tgenabled = 'O' AS enabled,
  pg_get_triggerdef(t.oid) as definition,
  concat (np.nspname, '.', p.proname) AS procedure
FROM pg_catalog.pg_trigger t
JOIN pg_catalog.pg_class c ON t.tgrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid  
JOIN pg_catalog.pg_proc p ON t.tgfoid = p.oid
JOIN pg_catalog.pg_namespace np ON p.pronamespace = np.oid
WHERE t.tgconstraint = 0  AND n.nspname IN (select * from unnest(current_schemas(false)))
ORDER BY 1,2,3
|]


{- tgtype is the type (INSERT, UPDATE) 
   tgattr is which column
 -}
data DbTrigger = DbTrigger { schema :: String, relation :: String, name :: String, enabled :: Bool,
                             procedure :: String, acl :: [Acl] }
  deriving(Show)
mkdbv (a:b:c:d:_) = DbView a b c (cvtacl d)

instance Show (Comparison DbView) where
    show (Equal x) = concat [sok, showView x,  treset]
    show (LeftOnly a) = concat [azure, [charLeftArrow]," ", showView a, treset]
    show (RightOnly a) = concat [peach, [charRightArrow], " ", showView a,  treset]
    show (Unequal a b) = concat [nok, showView a,  treset, 
         -- if (acl a /= acl b) then concat[ setAttr bold, "\n  acls: " , treset, map show $ dbCompare a b] else "",
         showAclDiffs (acl a) (acl b),
         if (compareIgnoringWhiteSpace (definition a) (definition b)) then ""
            else concat [setAttr bold,"\n  definition differences: \n", treset, concatMap show $ diff (lines $ definition a) (lines $ definition b)]
         ]

instance Comparable DbView where
  objCmp a b =
    if (acl a == acl b && compareIgnoringWhiteSpace (definition a) (definition b)) then Equal a
    else Unequal a b

compareViews (get1, get2, schemas) = do
    aa <- get1 viewList
    -- aac <- get1 viewColumns
    -- aat <- get1 viewTriggers
    -- aar <- get1 viewRules

    bb <- get2 viewList
    -- bbc <- get2 viewColumns
    -- bbt <- get2 viewTriggers
    -- bbr <- get2 viewRules

    let a = map (mkdbv . (map gs)) aa
    let b = map (mkdbv . (map gs)) bb

    let cc = dbCompare a b
    let cnt = dcount iseq cc
    putStr $ if (fst cnt > 0) then sok ++ (show $ fst cnt) ++ " matches, " else ""
    putStrLn $ if (snd cnt > 0) then concat [setColor dullRed,show $ snd cnt," differences"] else concat [sok,"no differences"]
    putStr $ treset
    return $ filter (not . iseq) cc

showView x = (schema x) ++ "." ++ (name x) 

instance Ord DbView where
  compare a b = let hd p = map ($ p) [schema, name] in compare (hd a) (hd b)

instance Eq DbView where
  (==) a b = EQ == compare a b
