{-# LANGUAGE QuasiQuotes, FlexibleInstances #-}

module View where

import Str(str)
import Acl
import Util
import Console
import Diff

	-- LEFT JOIN pg_catalog.pg_class dc ON (d.classoid=dc.oid AND dc.relname='pg_class')
	-- LEFT JOIN pg_catalog.pg_namespace dn ON (dn.oid=dc.relnamespace AND dn.nspname='pg_catalog')
viewList = [str| 
SELECT n.nspname AS "Schema", c.relname AS "Name", -- d.description AS "Comment",
  pg_get_viewdef(c.oid) AS definition,
  relacl AS "ACLs"
FROM pg_catalog.pg_namespace n
  JOIN pg_catalog.pg_class c ON c.relnamespace = n.oid 
  LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0)
WHERE n.nspname IN (select * from unnest(current_schemas(false)))
  AND c.relkind = 'v'
  AND n.nspname !~ '^pg_'
  AND n.nspname <> 'information_schema'
ORDER BY 1, 2
|]

viewColumns = [str|
SELECT n.nspname as "Schema",c.relname AS "View",a.attname AS "Column",a.atttypid AS "Type",
  a.attnotnull OR (t.typtype = 'd' AND t.typnotnull) AS attnotnull,
  a.atttypmod,a.attlen,row_number() OVER (PARTITION BY a.attrelid ORDER BY a.attnum) AS attnum,
  pg_catalog.pg_get_expr(def.adbin, def.adrelid) AS adsrc,
  dsc.description,t.typbasetype,t.typtype
FROM pg_catalog.pg_namespace n
  JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid)
  JOIN pg_catalog.pg_attribute a ON (a.attrelid=c.oid)
  JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid)
  LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid=def.adrelid AND a.attnum = def.adnum)
  LEFT JOIN pg_catalog.pg_description dsc ON (c.oid=dsc.objoid AND a.attnum = dsc.objsubid)
  LEFT JOIN pg_catalog.pg_class dc ON (dc.oid=dsc.classoid AND dc.relname='pg_class')
  LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace=dn.oid AND dn.nspname='pg_catalog') 
WHERE a.attnum > 0 AND NOT a.attisdropped
  AND n.nspname IN (select * from unnest(current_schemas(false)))
ORDER BY 1,2,3
|]

viewTriggers = [str|
SELECT n.nspname as "Schema", c.relname AS "View", t.tgname AS "Name", t.tgenabled = 'O' AS enabled,
  -- pg_get_triggerdef(trig.oid) as source
  concat (np.nspname, '.', p.proname) AS procedure
FROM pg_catalog.pg_trigger t
JOIN pg_catalog.pg_class c ON t.tgrelid = c.oid
JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid  
JOIN pg_catalog.pg_proc p ON t.tgfoid = p.oid
JOIN pg_catalog.pg_namespace np ON p.pronamespace = np.oid
WHERE t.tgconstraint = 0  AND n.nspname IN (select * from unnest(current_schemas(false)))
ORDER BY 1,2,3
|]

viewRules = [str|
SELECT n.nspname as "Schema", c.relname AS "View", r.rulename AS "Name", pg_get_ruledef(r.oid) AS definition
FROM pg_rewrite r
JOIN pg_class c ON c.oid = r.ev_class
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname IN (select * from unnest(current_schemas(false))) AND c.relkind = 'v'
ORDER BY 1,2,3
|]

data DbView = DbView { schema :: String, name :: String, definition :: String, acl :: [Acl] }
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


compareViews:: (String -> IO [PgResult], String -> IO [PgResult]) -> IO [Comparison DbView]
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
