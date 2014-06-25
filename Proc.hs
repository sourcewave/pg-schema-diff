{-# LANGUAGE QuasiQuotes, FlexibleInstances #-}

module Proc where

import PostgreSQL
import Str(str)
import Acl
import Util
import Diff

functionList :: String
functionList = [str|
SELECT n.nspname as "Schema",
  p.proname as "Name",
  pg_catalog.pg_get_function_arguments(p.oid) as "Argument data types",
  pg_catalog.pg_get_function_result(p.oid) as "Result data type",
 CASE
  WHEN p.proisagg THEN 'agg'
  WHEN p.proiswindow THEN 'window'
  WHEN p.prorettype = 'pg_catalog.trigger'::pg_catalog.regtype THEN 'trigger'
  ELSE 'normal'
 END as "Type",
 p.prosrc as "Source",
 p.proacl::varchar as "ACL"
FROM pg_catalog.pg_proc p
     LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
WHERE pg_catalog.pg_function_is_visible(p.oid)
      AND n.nspname <> 'pg_catalog'
      AND n.nspname <> 'information_schema'
ORDER BY 1, 2, 3;
|]

data DbProc = DbProc { schema :: String, name :: String, argTypes :: String, resType :: String, ptype :: String,
                       source :: String, acl :: [Acl] } deriving(Show, Eq)

mkdbp :: [String] -> DbProc
mkdbp (a:b:c:d:e:f:g:_) = DbProc a b c d e f (cvtacl g)

showProc :: DbProc -> String
showProc x = (schema x) ++ "." ++ (name x) ++ "(" ++ (argTypes x) ++ ")"

instance Show (Comparison DbProc) where
  show (Equal x) = concat [sok, showProc x,  treset]
  show (LeftOnly a) = concat [azure, [charLeftArrow]," ", showProc a, treset]
  show (RightOnly a) = concat [peach, [charRightArrow], " ", showProc a,  treset]
  show (Unequal a b) = concat [nok, showProc a,  treset, 
       if (resType a /= resType b) then concat [setAttr bold,"\n  resultTypes: ",treset, resType a, neq , resType b] else "",
       -- if (acl a /= acl b) then concat[ setAttr bold, "\n  acls: " , treset, intercalate ", " $ acl a, neq,  intercalate ", " $ acl b] else "",
       if (compareIgnoringWhiteSpace (source a) (source b)) then ""
          else concat [setAttr bold,"\n  source differences: \n", treset, concatMap show $ diff (lines $ source a) (lines $ source b)]
       ]

instance Comparable DbProc where
  objCmp a b = 
    if (resType a == resType b && acl a == acl b && compareIgnoringWhiteSpace (source a) (source b)) then Equal a
    else Unequal a b

compareProcs :: (String -> IO [PgResult], String -> IO [PgResult]) -> IO [Comparison DbProc]
compareProcs (get1, get2) = do
    aa <- get1 functionList
    let a = map (mkdbp . (map gs)) [aa]

    bb <- get2 functionList
    let b = map (mkdbp . (map gs)) [bb]

    let cc = dbCompare a b

    let cnt = dcount iseq cc

    putStr $ if (fst cnt > 0) then sok ++ (show $ fst cnt) ++ " matches, " else ""
    putStrLn $ if (snd cnt > 0) then concat [setColor dullRed,show $ snd cnt," differences"] else concat [sok,"no differences"]
    putStr $ treset
    return $ filter (not . iseq) cc

instance Ord DbProc where
  compare a b = let hd p = map ($ p) [schema, name, argTypes] in compare (hd a) (hd b)

