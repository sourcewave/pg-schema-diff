{-# LANGUAGE FlexibleInstances #-}

import Sqlcmds
import Database.HDBC
import Database.HDBC.PostgreSQL
import System.Environment
import Acl
import Proc
import View
import Util



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
    return (get1, get2, ra)

main = do
  args <- getArgs
  (get1, get2, schemas) <- initialize args

  -- compareProcs (get1, get2) >>= mapM print 
  
  compareViews (get1, get2, schemas) >>= mapM print
  
  -- disconnect conn1
  -- disconnect conn2

