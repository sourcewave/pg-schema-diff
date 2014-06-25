{-# LANGUAGE FlexibleInstances, QuasiQuotes, OverloadedStrings #-}

import PostgreSQL
import System.Environment
import Acl
import Proc
import View
-- import XferData
-- import Table
-- import UDT
import Trigger
import Util
-- import qualified MD5 
-- import qualified Crypto.Hash.MD5 as MD5
import MD5

import Str

import qualified Data.ByteString as B
import qualified Data.Text.Encoding as T
import qualified Data.Text as T

import Debug.Trace

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

    conn1 <- connectTo conns1 5432
    conn2 <- connectTo conns2 5432

    let username = "xxx"
    let password = "yyy" 
    let database = "zzz"

    [Authentication au1 au1x] <- doQuery conn1 (StartUpMessage [("user", username),("database",database)])
    traceShow (au1,au1x) $ return ()

    [Authentication au2 au2x] <- doQuery conn2 (StartUpMessage [("user", username),("database",database)])
    traceShow (au2,au2x) $ return ()

    if au1 == 5 then do
      j <- doQuery conn1 (Password password username au1x)
      print j
    else if au1 == 3 then do
      j <- doQuery conn1 (Password password undefined undefined)
      print j
    else do
      j <- undefined
      print (j :: String)



    if au2 == 5 then do
      j <- doQuery conn2 (Password password username au2x)
      print j
    else if au2 == 3 then do
      j <- doQuery conn2 (Password password undefined undefined)
      print j
    else do
      j <- undefined
      print (j :: String)


    let get1 x = doQuery conn1 (Query x)
    let get2 x = doQuery conn2 (Query x)
    ra <- ( if (null restArgs) then do
                sn1 <- get1 schemaList
                return $ (map (gs . head) [sn1])
            else do return restArgs)

    let searchPath = "set search_path="++ (intercalate "," ra)
    get1 searchPath
    get2 searchPath
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
--     "views" -> initialize ag >>= compareViews >>= mapM print
--     "triggers" -> initialize ag >>= compareTriggers >>= mapM print
     -- "xfer" -> initialize ag >>= xferData >>= mapM print
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

