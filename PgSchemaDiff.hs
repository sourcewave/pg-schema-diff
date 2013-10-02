{-# LANGUAGE FlexibleInstances, QuasiQuotes #-}

import Database.HDBC
import Database.HDBC.PostgreSQL
import System.Environment
import Acl
import Proc
import View
-- import Table
-- import UDT
import Trigger
import Util
import Str


schemaList = [str|
SELECT n.nspname AS "Name"
 -- ,pg_catalog.pg_get_userbyid(n.nspowner) AS "Owner"
FROM pg_catalog.pg_namespace n
WHERE n.nspname !~ '^pg_' AND n.nspname <> 'information_schema'
ORDER BY 1;
|]

initialize args = do
    let conns1 = head args
    let conns2 = (head . tail) args
    let restArgs = (drop 2 args)

    conn1 <- connectPostgreSQL conns1 -- "host=localhost user=r0ml dbname=storewave"
    conn2 <- connectPostgreSQL conns2 -- "host=localhost port=5433 user=never-use dbname=storewave"
    let get1 x = quickQuery' conn1 x []
    let get2 x = quickQuery' conn2 x []
    ra <- ( if (null restArgs) then do
                sn1 <- get1 schemaList
                return $ (map (gs . head) sn1)
            else do return restArgs)

    let searchPath = "set search_path="++ (intercalate "," ra)
    run conn1 searchPath []
    run conn2 searchPath []
    return (get1, get2)

{- The command line arguments are:
   1) a comma separated list of which things to diff 
   2) the first connection string
   3) the second connection string
   4) the list of schemas to compare
-}

main = do
  args <- getArgs
  let which = head args
      ag = tail args

  case which of
     "procs" -> initialize ag >>= compareProcs >>= mapM print 
     "views" -> initialize ag >>= compareViews >>= mapM print
     "triggers" -> initialize ag >>= compareTriggers >>= mapM print
     -- "tables" -> initialize ag >>= compareTables >>= mapM print
     -- "types" -> initialize ag >>= compareTypes >>= mapM print
     otherwise -> mapM putStr [ [str|
The valid comparisons are: procs, views, triggers, tables, types

The arguments are:
  comparisonType orangeConnectionString blueConnectionString schema1 schema2

The connectionStrings are of the form:
  "host={hostname} port={portNumber} user={userid} dbname={dbname}"
|] ]


  -- disconnect conn1
  -- disconnect conn2

