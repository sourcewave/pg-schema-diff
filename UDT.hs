{-# LANGUAGE QuasiQuotes, FlexibleInstances #-}

module Trigger where

import Str(str)
import Util
import Console
import Diff
import Data.Bits
import Debug.Trace

{-
typeList = [str|
SELECT typname FROM pg_catalog.pg_type WHERE oid = $1

SELECT typinput='array_in'::regproc, typtype FROM pg_catalog.pg_type WHERE typname = $1

select t.oid, t.typname, s.nspname from pg_type t, pg_namespace s where t.typnamespace = s.oid
select t.typname as udtname, ct.typname as datatype, a.atttypmod as len, t.oid, 
a.attname as name from pg_catalog.pg_type t, pg_catalog.pg_class c, pg_catalog.pg_attribute a,
 pg_catalog.pg_type ct, pg_catalog.pg_namespace sch  

where t.typtype = 'c' and t.typnamespace = sch.oid and t.typrelid = c.oid 
and sch.nspname = $1 and c.relkind = 'c' and c.oid = a.attrelid 
and a.atttypid = ct.oid order by t.typname, a.attnum

|]
-}



{-
SELECT n.nspname as "Schema",
  pg_catalog.format_type(t.oid, NULL) AS "Name",
  t.typname AS "Internal name",
  CASE WHEN t.typrelid != 0
      THEN CAST('tuple' AS pg_catalog.text)
    WHEN t.typlen < 0
      THEN CAST('var' AS pg_catalog.text)
    ELSE CAST(t.typlen AS pg_catalog.text)
  END AS "Size",
  pg_catalog.array_to_string(
      ARRAY(
		     SELECT e.enumlabel
          FROM pg_catalog.pg_enum e
          WHERE e.enumtypid = t.oid
          ORDER BY e.enumsortorder
      ),
      E'\n'
  ) AS "Elements",
pg_catalog.array_to_string(t.typacl, E'\n') AS "Access privileges",
    pg_catalog.obj_description(t.oid, 'pg_type') as "Description"
FROM pg_catalog.pg_type t
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
  AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
  AND (t.typname ~ '^(role_type)$'
        OR pg_catalog.format_type(t.oid, NULL) ~ '^(role_type)$')
  AND n.nspname ~ '^(account)$'
ORDER BY 1, 2;
-}


typeList = [str|
SELECT n.nspname as "Schema", c.relname AS "Relation", t.typname AS "Name"
FROM pg_catalog.pg_type t
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
