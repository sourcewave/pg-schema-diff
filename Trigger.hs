{-# LANGUAGE QuasiQuotes, FlexibleInstances #-}

module Trigger where

import Str(str)
import Util
import Console
import Diff
import Data.Bits
import Debug.Trace

triggerList = [str|
SELECT n.nspname as "Schema", c.relname AS "Relation", t.tgname AS "Name", tgtype AS "Type", t.tgenabled = 'O' AS enabled,
  concat (np.nspname, '.', p.proname) AS procedure,
  pg_get_triggerdef(t.oid) as definition
FROM pg_catalog.pg_trigger t
JOIN pg_catalog.pg_class c ON t.tgrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid  
JOIN pg_catalog.pg_proc p ON t.tgfoid = p.oid
JOIN pg_catalog.pg_namespace np ON p.pronamespace = np.oid
WHERE t.tgconstraint = 0  AND n.nspname IN (select * from unnest(current_schemas(false)))
ORDER BY 1,2,3
|]

data TriggerWhen = After | Before | InsteadOf deriving (Show, Eq)
data TriggerWhat = Insert | Delete | Update | Truncate deriving (Show, Eq)
data TriggerType = TriggerType TriggerWhen [TriggerWhat] TriggerHow deriving (Show, Eq) 
data TriggerHow = ForEachRow | ForEachStatement deriving (Show, Eq)

mktt x = let w = if testBit x 1 then Before else if testBit x 6 then InsteadOf else After
             t = map snd $ filter (\(b,z) -> testBit x b) $ [(2,Insert), (3,Delete), (4,Update), (5,Truncate)]
             h = if testBit x 0 then ForEachRow else ForEachStatement
         in TriggerType w t h

{- tgtype is the type (INSERT, UPDATE) 
   tgattr is which column
 -}
data DbTrigger = DbTrigger { schema :: String, relation :: String, name :: String, triggerType :: TriggerType, enabled :: Bool,
                             procedure :: String, definition :: String }
  deriving(Show)

mkdbt (a:b:c:d:e:f:g:_) = DbTrigger (gs a) (gs b) (gs c) (mktt (gi d)) (gb e) (gs f) (gs g)

instance Show (Comparison DbTrigger) where
    show (Equal x) = concat [sok, showTrigger x,  treset]
    show (LeftOnly a) = concat [azure, [charLeftArrow]," ", showTrigger a, treset]
    show (RightOnly a) = concat [peach, [charRightArrow], " ", showTrigger a,  treset]
    show (Unequal a b) = concat [nok, showTrigger a,  treset, 
         if compareIgnoringWhiteSpace (definition a) (definition b) then ""
            else concat [setAttr bold,"\n  definition differences: \n", treset, concatMap show $ diff (definition a) (definition b)]
         ]

instance Comparable DbTrigger where
  objCmp a b =
    if compareIgnoringWhiteSpace (definition a) (definition b) then Equal a
    else Unequal a b

compareTriggers (get1, get2) = do
    aa <- get1 triggerList
    -- aac <- get1 viewColumns
    -- aat <- get1 viewTriggers
    -- aar <- get1 viewRules

    bb <- get2 triggerList
    -- bbc <- get2 viewColumns
    -- bbt <- get2 viewTriggers
    -- bbr <- get2 viewRules

    let a = map mkdbt aa
    let b = map mkdbt bb

    let cc = dbCompare a b
    let cnt = dcount iseq cc
    putStr $ if fst cnt > 0 then sok ++ show (fst cnt) ++ " matches, " else ""
    putStrLn $ if snd cnt > 0 then concat [setColor dullRed,show $ snd cnt," differences"] else concat [sok,"no differences"]
    putStr treset
    return $ filter (not . iseq) cc

showTrigger x = concat [schema x, ".", relation x, "." , name x]

instance Ord DbTrigger where
  compare a b = let hd p = map ($ p) [schema, relation, name] in compare (hd a) (hd b)

instance Eq DbTrigger where
  (==) a b = EQ == compare a b
