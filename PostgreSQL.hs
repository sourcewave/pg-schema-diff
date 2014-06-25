{-# LANGUAGE OverloadedStrings #-}

module PostgreSQL 
where

import Control.Exception
import Data.Monoid (Monoid, mappend, mempty, mconcat)
import Data.Char (ord, chr)
import Control.Applicative ( (<*), (<*>), (<$>), pure )
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString.Lazy as B (toStrict, fromChunks)
import qualified Data.ByteString as B
import Data.ByteString (ByteString)
import qualified Network.Socket.ByteString as S (recv, send)
import qualified Network.Socket as S (accept, sClose, withSocketsDo, Socket(..),
   addrAddress, connect, defaultProtocol, SocketType(..), Family(..), getAddrInfo, defaultHints,
   socket, addrSocketType, addrFamily ) 
import Data.Int (Int8, Int16, Int32, Int64)
import MD5

import Control.Concurrent ( forkIO, newQSem, waitQSem, signalQSem, MVar, Chan,
    newChan, writeChan, readChan, newEmptyMVar, putMVar, takeMVar, QSem,
    threadDelay )
import qualified Data.Text as T (pack, unpack, singleton)
import qualified Data.Text.Encoding as T (encodeUtf8, decodeUtf8)

import qualified Data.ByteString.Builder as BB
import Debug.Trace

-- Copied from SCGI (could be collapsed)
data Postgres = Postgres { pgSend :: Chan PgQuery, pgRecv :: Chan PgResult }

type PgQuery = PgMessage

{-
data MessageType =
    CommandComplete
  | RowDescription
  | DataRow
  | EmptyQueryResponse
  | ErrorResponse
  | ReadyForQuery
  | NoticeResponse
  | AuthenticationOk
  | QueryMessage
  | PasswordMessage
  | UnknownMessageType
    deriving (Show,Eq)

instance Enum MessageType where
  toEnum x = maybe UnknownMessageType id (lookup (fromEnum x) messageTypeCodes)
  fromEnum typ = maybe 0 id (fmap fst $ find ((==typ).snd) messageTypeCodes)


-}

type Parameter = String -- placeholder
type Argument = String -- placeholder
type DataType = Int -- placeholder
data FieldDef = FieldDef String Int Int Int Int Int Int deriving (Show) -- placeholder
type FieldValue = Maybe ByteString -- placeholder
type Oid = Int -- placeholder ?
data PortalOrPrepared = Portal String | Prepared String deriving (Show)

data PgMessage = StartUpMessage [(String,String)]
    | Bind String String [FieldValue]
    | CancelRequest Int Int
    | CloseRequest PortalOrPrepared
    | CopyData ByteString
    | CopyDone
    | CopyInResponse Bool [Bool]
    | CopyOutResponse Bool [Bool]
    | CopyBothResponse Bool [Bool] 
    | CopyFail String
    | Describe PortalOrPrepared
    | Execute String Int
    | Flush
    | FunctionCall Oid [FieldValue]
    | Parse String String [DataType]
    | Password String String ByteString -- might be MD5'd
    | Query String
    | SSLRequest
    | Sync
    | Terminate
    | Authentication Int ByteString
    | ParameterStatus String String
    | BackendKey Int Int
    | ReadyForQuery Int
    | RowDescription [FieldDef]
    | DataRow [FieldValue]
    | FunctionResult FieldValue
    | CommandComplete String
    | ErrorResponse {- Severity SQLState Message -} [(Char,String)]
    | NoticeResponse [(Char,String)]
    | Notification Int String String
    | ParseComplete
    | BindComplete
    | CloseComplete
    | EmptyQuery
    | NoData
    | PortalSuspended
    deriving (Show)

-- newtype Severity = Error | Fatal | Panic | Warning | Notice | Debug | Info | Log
type Severity = String
type SQLState = String
type Message = String

  
getFieldData :: Int -> Get (Maybe ByteString)
getFieldData _n = do
  a <- getInt32
  if a == -1 then return Nothing else do
    b <- getByteString (fromIntegral a)
    return (Just b)
    

getUntil :: Word8 -> Get ByteString
getUntil c = fmap (B.toStrict  . BB.toLazyByteString) (getUntil' c mempty)
  where getUntil' cx a = do
          n <- getWord8
          if n == cx then return a else getUntil' cx (a `mappend` BB.word8 n)

getMessageComponent :: Get (Char,String)
getMessageComponent = do
    n <- getWord8
    b <- getUntil 0
    traceShow (n, b) $ return ( chr (fromIntegral n) , (T.unpack . T.decodeUtf8) b )
    
getMessageComponents :: Get [(Char, String)]
getMessageComponents = getMessageComponents' []
  where getMessageComponents' a = do
          b <- lookAhead getWord8
          if b == 0 then getWord8 >> return a else do 
            c <- getMessageComponent
            getMessageComponents' (c : a)

getFieldDef :: a -> Get FieldDef
getFieldDef _n = do
  a <- getUntil 0
  b <- getInt32
  c <- getInt16
  d <- getInt32
  e <- getInt16
  f <- getInt32
  g <- getInt16
  return $ FieldDef ((T.unpack . T.decodeUtf8) a) b c d e f g
  
instance Binary PgMessage where
  get = do
    b <- fmap (chr . fromIntegral) getWord8
    len <- getInt32 
    case b of 
      'R' -> do 
        au <- getInt32
        case au of
          0 -> return $ Authentication 0 B.empty
          3 -> return $ Authentication 3 B.empty
          5 -> do
            md <- getByteString 4
            return $ Authentication 5 md
      'S' -> do 
          a <- getByteString (len - 5)
          let [b,c] = B.split 0 a
          return $ ParameterStatus ((T.unpack . T.decodeUtf8) b) ((T.unpack . T.decodeUtf8) c)
      'K' -> BackendKey <$> fmap fromIntegral getWord32be <*> fmap fromIntegral getWord32be
      'Z' -> ReadyForQuery <$> fmap fromIntegral getWord8
      'T' -> do
          flds <- getInt16 -- the number of fields
          RowDescription <$> mapM getFieldDef [1..flds]
      'D' -> do
          flds <- getInt16 -- the number of fields
          DataRow <$> mapM getFieldData [1..flds]
      'C' -> do
          a <- getByteString (len - 5)
          return $ CommandComplete ((T.unpack . T.decodeUtf8) a)
      'V' -> FunctionResult <$> getFieldData 1
      'E' -> ErrorResponse <$> getMessageComponents
      '1' -> return ParseComplete
      '2' -> return BindComplete
      '3' -> return CloseComplete
      'd' -> CopyData <$> getByteString (len - 4)
      'c' -> return CopyDone
      's' -> return PortalSuspended
      'I' -> return EmptyQuery
      'n' -> return NoData
      'N' -> NoticeResponse <$> getMessageComponents
      'A' -> do
          proc <- fmap fromIntegral getWord32be
          chan <- getUntil 0
          pay <- getUntil 0
          return (Notification proc ((T.unpack . T.decodeUtf8) chan) ((T.unpack . T.decodeUtf8) pay))
      't' -> undefined -- ParameterDescription 
      _ -> undefined

  
{-
    
messageTypeCodes :: [(Int, MessageType)]
messageTypeCodes = map (\(x,y) -> ( ord x, y ) ) [
         ('B', BindMessage)
        ,('C', CloseRequest)
        ,('d', CopyDataRequest)
        ,('c', CopyDoneRequest)
        ,('f', CopyFail)
        ,('D', DescribeRequest)
        ,('E', ExecuteRequest)
        ,('H', FlushRequest)
        ,('F', FunctionCallRequest)
        ,('P', ParseRequest)
        ,('Q', QueryMessage)
        ,('p', PasswordMessage)
        ,('S', SyncRequest)
        ,('T', TerminateRequest)
        ]

backendMessageTypeCodes = map (\(x,y) -> ( ord x, y ) ) [
          ('R', AuthenticationMessage )
        , ('K', BackendKey )
        , ('2', BindCompleteMessage )
        , ('3', CloseCompleteMessage )
        , ('C', CommandComplete )
        , ('d', CopyDataMessage )
        , ('c', CopyDoneMessage )
        , ('G', CopyInResponseMessage )
        , ('H', CopyOutResponseMessage )
        , ('W', CopyBothResponseMessage )
        , ('D', DataRow )
        , ('I', EmptyQueryResponse )
        , ('E', ErrorResponse)
        , ('V', FunctionCallResponse )
        , ('n', NoDataResponse )
        , ('N', NoticeResponse )
        , ('A', NotificationResponse )
        , ('t', ParameterDescription )
        , ('S', ParameterStatus )
        , ('1', ParseComplete )
        , ('s', PortalSuspended)
        , ('Z', ReadyForQuery )
        , ('T', RowDescription )
        ]
-}

  put (Query s) = putByte 'Q' >> putStringMessage s

  put (StartUpMessage a) = do
     let m = concatMap (\(x,y) -> [(T.encodeUtf8 . T.pack) x, (T.encodeUtf8 . T.pack) y] ) a
         j = 9 + (foldl (\x y -> x + 1 + B.length y) 0 m)
     int32 j
     int32 protocolVersion
     mapM_ string m
     zero

  put Terminate = putByte 'X' >> putWord32be 4
  put Sync = putByte 'S' >> putWord32be 4
  put Flush = putByte 'H' >> putWord32be 4
  put SSLRequest = putByte 'F' >> putWord32be 8 >> putWord32be 80877103
  put (Password p u b) = let rp = B.concat ["md5", stringMD5 $ md5 ( B.concat [ stringMD5 $ md5 (B.concat [(T.encodeUtf8 . T.pack) p,(T.encodeUtf8 . T.pack) u]), b] )] 
                          in putByte 'p' >> putWord32be ( fromIntegral (B.length rp + 5)) >> putByteString rp >> zero


  put (Parse nam stmt typs) = do
    let namb = (T.encodeUtf8 . T.pack) nam
        stmtb = (T.encodeUtf8 . T.pack) stmt
    putByte 'P'
    putWord32be (fromIntegral (B.length namb + B.length stmtb + 8 + (4 * length typs)))
    putByteString namb >> zero
    putByteString stmtb >> zero
    putWord16be (fromIntegral (length typs))
    mapM_ (putWord32be . fromIntegral) typs
  
  put (FunctionCall oid fvs) = do
      putByte 'F'
      putWord32be (fromIntegral ml)
      putWord32be (fromIntegral oid)
      putWord16be 0 -- all arguments are text
      putWord16be (fromIntegral (length fvs))
      putByteString args
      putWord16be 0 -- result is text
    where args = B.toStrict (runPut (mapM_ put fvs))
          ml = 14 + B.length args

  put (Execute port max) = do
      putByte 'E'
      putWord32be (fromIntegral ml)
      putByteString namb
      zero
      putWord32be (fromIntegral max)
    where namb = (T.encodeUtf8 . T.pack) port
          ml = 9 + B.length namb
  
  put (Parse nam stmt dts) = do
      putByte 'P'
      putWord32be (fromIntegral ml)
      putByteString namb >> zero
      putByteString stmtb >> zero
      putWord16be (fromIntegral (length dts))
      mapM_ (putWord32be . fromIntegral) dts
    where namb = (T.encodeUtf8 . T.pack) nam
          stmtb = (T.encodeUtf8 . T.pack) stmt
          ml = (4*length dts) + 8 + B.length namb + B.length stmtb

  put (Bind nam stmt vals) = do
      putByte 'B'
      putWord32be (fromIntegral ml)
      putByteString namb >> zero
      putByteString stmtb >> zero
      putWord16be 0 -- all use text
      putWord16be (fromIntegral (length vals))
      putByteString args
      putWord16be 0 -- all use text
    where args = B.toStrict (runPut (mapM_ putFieldValue vals))
          namb = (T.encodeUtf8 . T.pack) nam
          stmtb = (T.encodeUtf8 . T.pack) stmt
          ml = B.length namb + B.length stmtb + 12 + B.length args
  
  put (CancelRequest proc key) = do
      putByte 'F'
      putWord32be 16
      putWord32be 80877102
      putWord32be (fromIntegral proc)
      putWord32be (fromIntegral key)
  
  put (CloseRequest x) = undefined
  
  put (CopyData x) = undefined
  
  put CopyDone = putByte 'c' >> putWord32be 4
  
  put (CopyFail x) = do
      putByte 'f'
      putWord32be (fromIntegral ml)
      putByteString a >> zero
    where a = (T.encodeUtf8 . T.pack) x
          ml = B.length a + 5
  
  put (Describe _) = undefined
  
  
  put _ = undefined

putStringMessage s = do
  let sb = (T.encodeUtf8 . T.pack) s
  putWord32be ( fromIntegral (B.length sb + 5))
  putByteString sb
  zero

putFieldValue fv = case fv of 
    Nothing -> putWord32be (fromIntegral (-1))
    Just b -> putWord32be (fromIntegral (B.length b)) >> putByteString b


putByte = putWord8 . (fromIntegral . ord) 

type PgResult = PgMessage
{-
data PgResult = PgResult {
    pgResultRows :: [[Maybe ByteString]]
   ,pgResultDesc :: Maybe [Field]
   ,pgResultError :: Maybe ByteString
   ,pgResultNotices :: [String]
   ,pgResultType :: MessageType
   ,pgResultTagRows :: Maybe Integer
  } deriving Show
-}

-- | A field description.
{-
data Field = Field {
    fieldType :: Type
   ,fieldFormatCode :: FormatCode
  } deriving Show

data Type = 
    Short      -- ^ 2 bytes, small-range integer
  | Long       -- ^ 4 bytes, usual choice for integer
  | LongLong   -- ^ 8 bytes	large-range integer
  | Decimal -- ^ variable, user-specified precision, exact, no limit
  | Numeric -- ^ variable, user-specified precision, exact, no limit
  | Real             -- ^ 4 bytes, variable-precision, inexact
  | DoublePrecision -- ^ 8 bytes, variable-precision, inexact

  | CharVarying -- ^ character varying(n), varchar(n), variable-length
  | Characters  -- ^ character(n), char(n), fixed-length
  | Text        -- ^ text, variable unlimited length
              -- 
              -- Lazy. Decoded from UTF-8 into Haskell native encoding.

  | Boolean -- ^ boolean, 1 byte, state of true or false

  | Timestamp -- ^ timestamp /without/ time zone
              -- 
              -- More information about PostgreSQL’s dates here:
              -- <http://www.postgresql.org/docs/current/static/datatype-datetime.html>
  | TimestampWithZone -- ^ timestamp /with/ time zone
  | Date              -- ^ date, 4 bytes	julian day
  | Time              -- ^ 8 bytes, time of day (no date)

   deriving (Eq,Enum,Show)
-}

-- | A field size.
data Size = Varying | Size Int16
  deriving (Eq,Ord,Show)

-- | A text format code. Will always be TextCode for DESCRIBE queries.
data FormatCode = TextCode | BinaryCode
  deriving (Eq,Ord,Show)

-- | A type-specific modifier.
data Modifier = Modifier

-- | A PostgreSQL object ID.
type ObjectId = Int
   
connectTo :: String -> Int -> IO Postgres -- host port path headers 
connectTo host port = do
    -- Create and connect socket
    let hints = S.defaultHints {S.addrFamily = S.AF_INET, S.addrSocketType = S.Stream}
    addrInfos <- S.getAddrInfo (Just hints) (Just host) (Just $ show port)
    sock      <- S.socket S.AF_INET S.Stream S.defaultProtocol

    qchan <- newChan  -- requests
    rchan <- newChan  -- responses
    
    _ <- forkIO $ finally
            (do { S.connect sock (S.addrAddress $ head addrInfos); sendReq qchan sock rchan })
            (S.sClose sock)
    return (Postgres qchan rchan)

-- getResponse :: Postgres -> IO PgResult
-- getResponse (Postgres a x) = readChan x

sendQuery :: Postgres -> PgMessage -> IO ()
sendQuery (Postgres z x) s = writeChan z s

doQuery :: Postgres -> PgMessage -> IO [PgResult]
doQuery (Postgres z x) s = do 
    writeChan z s
    getResults x []
  where getResults x accum = do
                    a <- readChan x
                    let z = return $ reverse ( a : accum )
                    case a of 
                        ReadyForQuery _ -> z
                        ParseComplete -> z
                        BindComplete -> z
                        PortalSuspended -> z
                        ErrorResponse _ -> z
                        CommandComplete _ -> z
                        Authentication _ _ -> z
                        _ -> getResults x (a : accum)

sendReq :: Chan PgQuery -> S.Socket -> Chan PgResult -> IO ()
sendReq c s rc = do
  print "calling sendreq"
  q <- readChan c
  print ("sending " ++ show q)

  sendBlock s q
  
  -- sendBlock s Flush
  
  print "sent"
  
  processResponse s rc 
  print "response processed"  
  
  sendReq c s rc -- tail recursion
  
processResponse :: S.Socket -> Chan PgResult -> IO ()
processResponse s rc = do
  a <- readResponse s
  print "received"
  writeChan rc a
  case a of 
    ReadyForQuery _ -> return ()
    ParseComplete -> return ()
    BindComplete -> return ()
    PortalSuspended -> return ()
    ErrorResponse _ -> return ()
    CommandComplete _ -> return ()
    Authentication _ _ -> return ()
    _ -> processResponse s rc

readResponse :: S.Socket -> IO PgResult
readResponse s = do
  more <- S.recv s 5 :: IO B.ByteString
  print ("more",more)
  if B.null more then error "pg_reader read nothing"
  else let typ = B.head more
           len = (runGet getInt32 (B.fromChunks [B.tail more])) - 4
        in do
             msg <- S.recv s len
             print ("got more",msg)
             if B.length msg == len then return $ runGet get (B.fromChunks [more, msg])
             else error "need to read more bytes"
  

protocolVersion = 0x30000

-- | Put a Haskell string, encoding it to UTF-8, and null-terminating it.
string :: ByteString -> Put
string s = putByteString s >> zero

-- | Put a Haskell 32-bit integer.
int32 :: Int -> Put
int32 = putWord32be . fromIntegral 

-- | Put zero-byte terminator.
zero :: Put
zero = putWord8 0

getInt32 :: Get Int
getInt32 = fmap fromIntegral (fmap fromIntegral getWord32be :: Get Int32)

getInt16 :: Get Int
getInt16 = fmap fromIntegral (fmap fromIntegral getWord16be :: Get Int16)


-- | Send a block of bytes on a handle, prepending the complete length.
sendBlock :: Binary a => S.Socket -> a -> IO ()
sendBlock h outp = sendAll h (B.toStrict (runPut (put outp)))

sendAll h msg = do
  n <- S.send h msg
  if n <= 0 then error "failed to send"
  else let rm = B.drop n msg in if B.null rm then return () else sendAll h rm
{-

-- | Exception thrown if a 'Query' could not be formatted correctly.
-- This may occur if the number of \'@?@\' characters in the query
-- string does not match the number of parameters provided.
data FormatError = FormatError {
      fmtMessage :: String
    , fmtQuery :: Query
    , fmtParams :: [ByteString]
    } deriving (Eq, Show, Typeable)

instance Exception FormatError

-- | Exception thrown if 'query' is used to perform an @INSERT@-like
-- operation, or 'execute' is used to perform a @SELECT@-like operation.
data QueryError = QueryError {
      qeMessage :: String
    , qeQuery :: Query
    } deriving (Eq, Show, Typeable)

instance Exception QueryError

-- | Format a query string.
-- Throws 'FormatError' if the query string could not be formatted correctly.
formatQuery :: QueryParams q => Query -> q -> IO ByteString
formatQuery q@(Query template) qs
    | null xs && '?' `B.notElem` template = return template
    | otherwise = toByteString <$> buildQuery q template xs
  where xs = renderParams qs

-- | Format a query string with a variable number of rows.
--
-- This function is exposed to help with debugging and logging. Do not
-- use it to prepare queries for execution.
--
-- The query string must contain exactly one substitution group,
-- identified by the SQL keyword \"@VALUES@\" (case insensitive)
-- followed by an \"@(@\" character, a series of one or more \"@?@\"
-- characters separated by commas, and a \"@)@\" character. White
-- space in a substitution group is permitted.
--
-- Throws 'FormatError' if the query string could not be formatted
-- correctly.
formatMany :: (QueryParams q) => Query -> [q] -> IO ByteString
formatMany q [] = fmtError "no rows supplied" q []
formatMany q@(Query template) qs = do
  case match re template [] of
    Just [_,before,qbits,after] -> do
      bs <- mapM (buildQuery q qbits . renderParams) qs
      return . toByteString . mconcat $ fromByteString before :
                                        intersperse (fromChar ',') bs ++
                                        [fromByteString after]
    _ -> error "formatMany: The query did not match the documented format."
  where
   re = compile "^([^?]+\\bvalues\\s*)\
                 \(\\(\\s*[?](?:\\s*,\\s*[?])*\\s*\\))\
                 \([^?]*)$"
        [caseless]

buildQuery :: Query -> ByteString -> [Action] -> IO Builder
buildQuery q template xs = zipParams (split template) <$> mapM sub xs
  where sub (Plain b)  = pure b
        sub (Escape s) = pure $ (inQuotes . fromByteString . escapeBS) s
        sub (Many ys)  = mconcat <$> mapM sub ys
        split s = fromByteString h : if B.null t then [] else split (B.tail t)
            where (h,t) = B.break (=='?') s
        zipParams (t:ts) (p:ps) = t `mappend` p `mappend` zipParams ts ps
        zipParams [t] []        = t
        zipParams _ _ = fmtError (show (B.count '?' template) ++
                                  " '?' characters, but " ++
                                  show (length xs) ++ " parameters") q xs

-- | Execute an @INSERT@, @UPDATE@, or other SQL query that is not
-- expected to return results.
--
-- Returns the number of rows affected.
--
-- Throws 'FormatError' if the query could not be formatted correctly.
execute :: (QueryParams q) => Connection -> Query -> q -> IO Integer
execute conn template qs = do
  exec conn =<< formatQuery template qs

-- | A version of 'execute' that does not perform query substitution.
execute_ :: Connection -> Query -> IO Integer
execute_ conn (Query stmt) = do
  exec conn stmt

-- | Execute a multi-row @INSERT@, @UPDATE@, or other SQL query that is not
-- expected to return results.
--
-- Returns the number of rows affected.
--
-- Throws 'FormatError' if the query could not be formatted correctly.
executeMany :: (QueryParams q) => Connection -> Query -> [q] -> IO Integer
executeMany _ _ [] = return 0
executeMany conn q qs = do
  exec conn =<< formatMany q qs

-- | Perform a @SELECT@ or other SQL query that is expected to return
-- results. All results are retrieved and converted before this
-- function returns.
--
-- When processing large results, this function will consume a lot of
-- client-side memory.  Consider using 'fold' instead.
--
-- Exceptions that may be thrown:
--
-- * 'FormatError': the query string could not be formatted correctly.
--
-- * 'QueryError': the result contains no columns (i.e. you should be
--   using 'execute' instead of 'query').
--
-- * 'ResultError': result conversion failed.
query :: (QueryParams q, QueryResults r)
         => Connection -> Query -> q -> IO [r]
query conn template qs = do
  q <- formatQuery template qs
  (fields,rows) <- query conn q
  forM rows $ \row -> let c = convertResults fields row
                      in return c

-- | A version of 'query' that does not perform query substitution.
query_ :: (QueryResults r) => Connection -> Query -> IO [r]
query_ conn (Query q) = do
  (fields,rows) <- query conn q
  forM rows $ \row -> let c = convertResults fields row
                      in return c

-- | A processed, appendable. query
newtype ProcessedQuery r = ProcessedQuery ByteString
  deriving (Monoid,Show)

-- | Process a query for later use.
processQuery :: (QueryParams q,QueryResults r) => Query -> q -> IO (ProcessedQuery r)
processQuery template qs = fmap ProcessedQuery $ formatQuery template qs

-- | A version of 'query' that does not perform query substitution.
queryProcessed :: (QueryResults r) => Connection -> ProcessedQuery r -> IO [r]
queryProcessed conn (ProcessedQuery q) = do
  (fields,rows) <- query conn q
  forM rows $ \row -> let c = convertResults fields row
                      in return c

fmtError :: String -> Query -> [Action] -> a
fmtError msg q xs = throw FormatError {
                      fmtMessage = msg
                    , fmtQuery = q
                    , fmtParams = map twiddle xs
                    }
  where twiddle (Plain b)  = toByteString b
        twiddle (Escape s) = s
        twiddle (Many ys)  = B.concat (map twiddle ys)




-- Use as in the following example:
-- > connect defaultConnectInfo { connectHost = "db.example.com" }
defaultConnectInfo :: ConnectInfo
defaultConnectInfo = ConnectInfo {
                       connectHost = "127.0.0.1"
                     , connectPort = 5432
                     , connectUser = "postgres"
                     , connectPassword = ""
                     , connectDatabase = ""
                     }

-- | Connect with the given username to the given database. Will throw
--   an exception if it cannot connect.
connect :: MonadIO m => ConnectInfo -> m Connection -- ^ The datase connection.
connect connectInfo@ConnectInfo{..} = liftIO $ withSocketsDo $ do
  var <- newEmptyMVar
  h <- connectTo connectHost (PortNumber $ fromIntegral connectPort)
  hSetBuffering h NoBuffering
  putMVar var $ Just h
  types <- newMVar IntMap.empty
  let conn = Connection var types
  authenticate conn connectInfo
  return conn

-- | Run a an action with a connection and close the connection
--   afterwards (protects against exceptions).
withDB :: (MonadCatchIO m,MonadIO m) => ConnectInfo -> (Connection -> m a) -> m a
withDB connectInfo m = E.bracket (liftIO $ connect connectInfo) (liftIO . close) m

-- | With a transaction, do some action (protects against exceptions).
withTransaction :: (MonadCatchIO m,MonadIO m) => Connection -> m a -> m a
withTransaction conn act = do
  begin conn
  r <- act `E.onException` rollback conn
  commit conn
  return r

-- | Rollback a transaction.
rollback :: (MonadCatchIO m,MonadIO m) => Connection -> m ()
rollback conn = do
  _ <- exec conn (fromString ("ABORT;" :: String))
  return ()

-- | Commit a transaction.
commit :: (MonadCatchIO m,MonadIO m) => Connection -> m ()
commit conn = do
  _ <- exec conn (fromString ("COMMIT;" :: String))
  return ()

-- | Begin a transaction.
begin :: (MonadCatchIO m,MonadIO m) => Connection -> m ()
begin conn = do
  _ <- exec conn (fromString ("BEGIN;" :: String))
  return ()

-- | Close a connection. Can safely be called any number of times.
close :: MonadIO m => Connection -- ^ The connection.
      -> m ()
close Connection{connectionHandle} = liftIO$ do
  modifyMVar_ connectionHandle $ \h -> do
    case h of
      Just h -> hClose h
      Nothing -> return ()
    return Nothing

-- | Run a simple query on a connection.
query :: (MonadCatchIO m)
      => Connection -- ^ The connection.
      -> ByteString              -- ^ The query.
      -> m ([Field],[[Maybe ByteString]])
query conn sql = do
  result <- execQuery conn sql
  case result of
    (_,Just ok) -> return ok
    _           -> return ([],[])

-- | Run a simple query on a connection.
execQuery :: (MonadCatchIO m)
      => Connection -- ^ The connection.
      -> ByteString              -- ^ The query.
      -> m (Integer,Maybe ([Field],[[Maybe ByteString]]))
execQuery conn sql = liftIO $ do
  withConnection conn $ \h -> do
    types <- readMVar $ connectionObjects conn
    Result{..} <- sendQuery types h sql
    case resultType of
      ErrorResponse -> E.throw $ QueryError (fmap L.toString resultError)
      EmptyQueryResponse -> E.throw QueryEmpty
      _             ->
        let tagCount = fromMaybe 0 resultTagRows
        in case resultDesc of
             Just fields -> return $ (tagCount,Just (fields,resultRows))
             Nothing     -> return $ (tagCount,Nothing)

-- | Exec a command.
exec :: (MonadCatchIO m)
     => Connection
     -> ByteString
     -> m Integer
exec conn sql = do
  result <- execQuery conn sql
  case result of
    (ok,_) -> return ok

-- | Escape a string for PostgreSQL.
escape :: String -> String
escape ('\\':cs) = '\\' : '\\' : escape cs
escape ('\'':cs) = '\'' : '\'' : escape cs
escape (c:cs) = c : escape cs
escape [] = []

-- | Escape a string for PostgreSQL.
escapeBS :: ByteString -> ByteString
escapeBS = fromString . escape . toString

--------------------------------------------------------------------------------
-- Authentication

-- | Run the connectInfoentication procedure.
authenticate :: Connection -> ConnectInfo -> IO ()
authenticate conn@Connection{..} connectInfo = do
  withConnection conn $ \h -> do
    sendStartUp h connectInfo
    getConnectInfoResponse h connectInfo
    objects <- objectIds h
    modifyMVar_ connectionObjects (\_ -> return objects)

-- | Send the start-up message.

-- | Wait for and process the connectInfoentication response from the server.
getConnectInfoResponse :: Handle -> ConnectInfo -> IO ()
getConnectInfoResponse h conninfo = do
  (typ,block) <- getMessage h
  -- TODO: Handle connectInfo failure. Handle information messages that are
  --       sent, maybe store in the connection value for later
  --       inspection.
  case typ of
    AuthenticationOk
      | param == 0 -> waitForReady h
      | param == 3 -> sendPassClearText h conninfo
--      | param == 5 -> sendPassMd5 h conninfo salt
      | otherwise  -> E.throw $ UnsupportedAuthenticationMethod param (show block)
        where param = decode block :: Int32
              _salt = flip runGet block $ do
                        _ <- getInt32
                        getWord8
    
    els -> E.throw $ AuthenticationFailed (show (els,block))

-- | Send the pass as clear text and wait for connect response.
sendPassClearText :: Handle -> ConnectInfo -> IO ()
sendPassClearText h conninfo@ConnectInfo{..} = do
  sendMessage h PasswordMessage $
    string (fromString connectPassword)
  getConnectInfoResponse h conninfo

-- -- | Send the pass as salted MD5 and wait for connect response.
-- sendPassMd5 :: Handle -> ConnectInfo -> Word8 -> IO ()
-- sendPassMd5 h conninfo@ConnectInfo{..} salt = do
--   -- TODO: Need md5 library with salt support.
--   sendMessage h PasswordMessage $
--     string (fromString connectPassword)
--   getConnectInfoResponse h conninfo

--------------------------------------------------------------------------------
-- Initialization

objectIds :: Handle -> IO (IntMap Type)
objectIds h = do
    Result{..} <- sendQuery IntMap.empty h q
    case resultType of
      ErrorResponse -> E.throw $ InitializationError "Couldn't get types."
      _ -> return $ IntMap.fromList $ catMaybes $ flip map resultRows $ \row ->
             case map toString $ catMaybes row of
               [typeName,readMay -> Just objId] -> do
                 typ <- typeFromName typeName
	         return (fromIntegral objId,typ)
               _ -> Nothing

  where q = fromString ("SELECT typname, oid FROM pg_type" :: String)

--------------------------------------------------------------------------------
-- Queries and commands

-- | Send a simple query.
sendQuery :: IntMap Type -> Handle -> ByteString -> IO Result
sendQuery types h sql = do
  sendMessage h Query $ string sql
  listener $ \continue -> do
    (typ,block) <- liftIO $ getMessage h
    let setStatus = modify $ \r -> r { resultType = typ }
    case typ of
      ReadyForQuery ->
        modify $ \r -> r { resultRows = reverse (resultRows r) }

      listenPassively -> do
        case listenPassively of
          EmptyQueryResponse -> setStatus
          CommandComplete    -> do setStatus
                                   setCommandTag block
          ErrorResponse -> do
            modify $ \r -> r { resultError = Just block }
            setStatus
          RowDescription -> getRowDesc types block
          DataRow        -> getDataRow block
          _ -> return ()

        continue

  where emptyResponse = Result [] Nothing Nothing [] UnknownMessageType Nothing
        listener m = execStateT (fix m) emptyResponse

-- | CommandComplete returns a ‘tag’ which indicates how many rows were
-- affected, or returned, as a result of the command.
-- See http://developer.postgresql.org/pgdocs/postgres/protocol-message-formats.html
setCommandTag :: MonadState Result m => L.ByteString -> m ()
setCommandTag block = do
  modify $ \r -> r { resultTagRows = rows }
    where rows =
            case tag block of
              ["INSERT",_oid,readMay -> Just rows]         -> return rows
              [cmd,readMay -> Just rows] | cmd `elem` cmds -> return rows
              _                                            -> Nothing
          tag = words . concat . map toString . L.toChunks . runGet getString
          cmds = ["DELETE","UPDATE","SELECT","MOVE","FETCH"]

-- | Update the row description of the result.
getRowDesc :: MonadState Result m => IntMap Type -> L.ByteString -> m ()
getRowDesc types block =
  modify $ \r -> r {
    resultDesc = Just (parseFields types (runGet parseMsg block))
  }
    where parseMsg = do
            fieldCount :: Int16 <- getInt16
            forM [1..fieldCount] $ \_ -> do
              name <- getString
              objid <- getInt32
              colid <- getInt16
              dtype <- getInt32
              size <- getInt16
              modifier <- getInt32
              code <- getInt16
              return (name,objid,colid,dtype,size,modifier,code)

-- | Parse a row description.
--
-- Parts of the row description are:
--
-- String: The field name.
--
-- Int32: If the field can be identified as a column of a specific
-- table, the object ID of the table; otherwise zero.
--
-- Int16: If the field can be identified as a column of a specific
-- table, the attribute number of the column; otherwise zero.
----
-- Int32: The object ID of the field's data type.
----
-- Int16: The data type size (see pg_type.typlen). Note that negative
-- values denote variable-width types.
----
-- Int32: The type modifier (see pg_attribute.atttypmod). The meaning
-- of the modifier is type-specific.
--
-- Int16: The format code being used for the field. Currently will be
-- zero (text) or one (binary). In a RowDescription returned from the
-- statement variant of Describe, the format code is not yet known and
-- will always be zero.
--
parseFields :: IntMap Type
            -> [(L.ByteString,Int32,Int16,Int32,Int16,Int32,Int16)]
            -> [Field]
parseFields types = mapMaybe parse where
  parse (_fieldName
        ,_ -- parseObjId        -> _objectId
        ,_ -- parseAttrId       -> _attrId
        ,parseType types        -> typ
        ,_ -- parseSize         -> _typeSize
        ,_ -- parseModifier typ -> _typeModifier
        ,parseFormatCode   -> formatCode)
    = Just $ Field {
      fieldType = typ
    , fieldFormatCode = formatCode
    }

-- These aren't used (yet).

-- -- | Parse an object ID. 0 means no object.
-- parseObjId :: Int32 -> Maybe ObjectId
-- parseObjId 0 = Nothing
-- parseObjId n = Just (ObjectId n)

-- -- | Parse an attribute ID. 0 means no object.
-- parseAttrId :: Int16 -> Maybe ObjectId
-- parseAttrId 0 = Nothing
-- parseAttrId n = Just (ObjectId $ fromIntegral n)

-- | Parse a number into a type.
parseType :: IntMap Type -> Int32 -> Type
parseType types objId =
  case IntMap.lookup (fromIntegral objId) types of
    Just typ -> typ
    _ -> error $ "parseType: Unknown type given by object-id: " ++ show objId

typeFromName :: String -> Maybe Type
typeFromName = flip lookup fieldTypes

fieldTypes :: [(String, Type)]
fieldTypes =
  [("bool",Boolean)
  ,("int2",Short)
  ,("integer",Long)
  ,("int",Long)
  ,("int4",Long)
  ,("int8",LongLong)
  ,("timestamptz",TimestampWithZone)
  ,("varchar",CharVarying)
  ,("text",Text)]

-- This isn't used yet.
-- | Parse a type's size.
-- parseSize :: Int16 -> Size
-- parseSize (-1) = Varying
-- parseSize n    = Size n

-- This isn't used yet.
-- -- | Parse a type-specific modifier.
-- parseModifier :: Type -> Int32 -> Maybe Modifier
-- parseModifier _typ _modifier = Nothing

-- | Parse a format code (text or binary).
parseFormatCode :: Int16 -> FormatCode
parseFormatCode 1 = BinaryCode
parseFormatCode _ = TextCode

-- | Add a data row to the response.
getDataRow :: MonadState Result m => L.ByteString -> m ()
getDataRow block =
  modify $ \r -> r { resultRows = runGet parseMsg block : resultRows r }
    where parseMsg = do
            values :: Int16 <- getInt16
            forM [1..values] $ \_ -> do
              size <- getInt32
              if size == -1
                 then return Nothing
                 else do v <- getByteString (fromIntegral size)
                         return (Just v)

-- TODO:
-- getNotice :: MonadState Result m => L.ByteString -> m ()
-- getNotice block =
--   return ()
--  modify $ \r -> r { responseNotices = runGet parseMsg block : responseNotices r }
--    where parseMsg = return ""


-- | Blocks until receives ReadyForQuery.
waitForReady :: Handle -> IO ()
waitForReady h = loop where
  loop = do
  (typ,block) <- getMessage h
  case typ of
    ErrorResponse -> E.throw $ GeneralError $ show block
    ReadyForQuery | decode block == 'I' -> return ()
    _                                   -> loop

--------------------------------------------------------------------------------
-- Connections

-- | Atomically perform an action with the database handle, if there is one.
withConnection :: Connection -> (Handle -> IO a) -> IO a
withConnection Connection{..} m = do
  withMVar connectionHandle $ \h -> do
    case h of
      Just h -> m h
      -- TODO: Use extensible exceptions.
      Nothing -> E.throw ConnectionLost

-- | Send a block of bytes on a handle, prepending the message type
--   and complete length.
sendMessage :: Handle -> MessageType -> Put -> IO ()
sendMessage h typ output =
  case charFromType typ of
    Just char -> sendBlock h (Just char) output
    Nothing   -> error $ "sendMessage: Bad message type " ++ show typ
-}

{-
-- | Send a block of bytes on a handle, prepending the complete length.
sendBlock :: Handle -> Maybe Char -> Put -> IO ()
sendBlock h typ output = do
  L.hPutStr h bytes
    where bytes = start `mappend` out
          start = runPut $ do
            maybe (return ()) (put . toByte) typ
            int32 $ fromIntegral int32Size +
                    fromIntegral (L.length out)
          out = runPut output
          toByte c = fromIntegral (fromEnum c) :: Word8
-}
{-
-- | Get a message (block) from the stream.
getMessage :: Handle -> IO (MessageType,L.ByteString)
getMessage h = do
  messageType <- L.hGet h 1
  blockLength <- L.hGet h int32Size
  let typ = decode messageType
      rest = fromIntegral (decode blockLength :: Int32) - int32Size
  block <- L.hGet h rest
  return (maybe UnknownMessageType id $ typeFromChar typ,block)

--------------------------------------------------------------------------------
-- | To avoid magic numbers, size of a 32-bit integer in bytes.
int32Size :: Int
int32Size = 4

getInt16 :: Get Int16
getInt16 = get

getInt32 :: Get Int32
getInt32 = get

getString :: Get L.ByteString
getString = getLazyByteStringNul

readMay :: Read a => String -> Maybe a
readMay x = case reads x of
              [(v,"")] -> return v
              _        -> Nothing







data ConnectionError =
    QueryError2 (Maybe String)   -- ^ Query returned an error.
  | QueryEmpty                  -- ^ The query was empty.
  | AuthenticationFailed String -- ^ Connecting failed due to authentication problem.
  | InitializationError String  -- ^ Initialization (e.g. getting data types) failed.
  | ConnectionLost              -- ^ Connection was lost when using withConnection.
  | UnsupportedAuthenticationMethod Int32 String -- ^ Unsupported method of authentication (e.g. md5).
  | GeneralError String
  deriving (Typeable,Show)

instance Exception ConnectionError where

-- | Connection configuration.
data ConnectInfo = ConnectInfo {
      connectHost :: String
    , connectPort :: Word16
    , connectUser :: String
    , connectPassword :: String
    , connectDatabase :: String
    } deriving (Eq,Read,Show,Typeable)

-- | A database connection.
data Connection = Connection {
      connectionHandle  :: MVar (Maybe Handle)
    , connectionObjects :: MVar (IntMap Type)
    }

-- | Result of a database query.


-- | An internal message type.


-- | How to render an element when substituting it into a query.
data Action =
    Plain Builder
    -- ^ Render without escaping or quoting. Use for non-text types
    -- such as numbers, when you are /certain/ that they will not
    -- introduce formatting vulnerabilities via use of characters such
    -- as spaces or \"@'@\".
  | Escape ByteString
    -- ^ Escape and enclose in quotes before substituting. Use for all
    -- text-like types, and anything else that may contain unsafe
    -- characters when rendered.
  | Many [Action]
    -- ^ Concatenate a series of rendering actions.
    deriving (Typeable)

instance Show Action where
    show (Plain b)  = "Plain " ++ show (toByteString b)
    show (Escape b) = "Escape " ++ show b
    show (Many b)   = "Many " ++ show b

-- | A type that may be used as a single parameter to a SQL query.
class Param a where
    render :: a -> Action
    -- ^ Prepare a value for substitution into a query string.

instance Param Action where
    render a = a

instance (Param a) => Param (Maybe a) where
    render Nothing  = renderNull
    render (Just a) = render a

instance (Param a) => Param (In [a]) where
    render (In []) = Plain $ fromByteString "(null)"
    render (In xs) = Many $
        Plain (fromChar '(') :
        (intersperse (Plain (fromChar ',')) . map render $ xs) ++
        [Plain (fromChar ')')]

instance Param (Binary SB.ByteString) where
    render (Binary bs) = Plain $ fromByteString "x'" `mappend`
                                 fromByteString (B16.encode bs) `mappend`
                                 fromChar '\''

instance Param (Binary LB.ByteString) where
    render (Binary bs) = Plain $ fromByteString "x'" `mappend`
                                 fromLazyByteString (L16.encode bs) `mappend`
                                 fromChar '\''

renderNull :: Action
renderNull = Plain (fromByteString "null")

instance Param Null where
    render _ = renderNull

instance Param Bool where
    render = Plain . integral . fromEnum

instance Param Int8 where
    render = Plain . integral

instance Param Int16 where
    render = Plain . integral

instance Param Int32 where
    render = Plain . integral

instance Param Int where
    render = Plain . integral

instance Param Int64 where
    render = Plain . integral

instance Param Integer where
    render = Plain . integral

instance Param Word8 where
    render = Plain . integral

instance Param Word16 where
    render = Plain . integral

instance Param Word32 where
    render = Plain . integral

instance Param Word where
    render = Plain . integral

instance Param Word64 where
    render = Plain . integral

instance Param Float where
    render v | isNaN v || isInfinite v = renderNull
             | otherwise               = Plain (float v)

instance Param Double where
    render v | isNaN v || isInfinite v = renderNull
             | otherwise               = Plain (double v)

instance Param SB.ByteString where
    render = Escape

instance Param LB.ByteString where
    render = render . SB.concat . LB.toChunks

instance Param ST.Text where
    render = Escape . ST.encodeUtf8

instance Param [Char] where
    render = Escape . toByteString . Utf8.fromString

instance Param LT.Text where
    render = render . LT.toStrict

instance Param UTCTime where
    render = Plain . Utf8.fromString . formatTime defaultTimeLocale "'%F %T'"

instance Param Day where
    render = Plain . inQuotes . Utf8.fromString . showGregorian

instance Param TimeOfDay where
    render = Plain . inQuotes . Utf8.fromString . show

-- | Surround a string with single-quote characters: \"@'@\"
--
-- This function /does not/ perform any other escaping.
inQuotes :: Builder -> Builder
inQuotes b = quote `mappend` b `mappend` quote
  where quote = Utf8.fromChar '\''



-- | A collection type that can be turned into a list of rendering
-- 'Action's.
--
-- Instances should use the 'render' method of the 'Param' class
-- to perform conversion of each element of the collection.
class QueryParams a where
    renderParams :: a -> [Action]
    -- ^ Render a collection of values.

instance QueryParams () where
    renderParams _ = []

instance (Param a) => QueryParams (Only a) where
    renderParams (Only v) = [render v]

instance (Param a, Param b) => QueryParams (a,b) where
    renderParams (a,b) = [render a, render b]

instance (Param a, Param b, Param c) => QueryParams (a,b,c) where
    renderParams (a,b,c) = [render a, render b, render c]

instance (Param a, Param b, Param c, Param d) => QueryParams (a,b,c,d) where
    renderParams (a,b,c,d) = [render a, render b, render c, render d]

instance (Param a, Param b, Param c, Param d, Param e)
    => QueryParams (a,b,c,d,e) where
    renderParams (a,b,c,d,e) =
        [render a, render b, render c, render d, render e]

instance (Param a, Param b, Param c, Param d, Param e, Param f)
    => QueryParams (a,b,c,d,e,f) where
    renderParams (a,b,c,d,e,f) =
        [render a, render b, render c, render d, render e, render f]

instance (Param a, Param b, Param c, Param d, Param e, Param f, Param g)
    => QueryParams (a,b,c,d,e,f,g) where
    renderParams (a,b,c,d,e,f,g) =
        [render a, render b, render c, render d, render e, render f, render g]

instance (Param a, Param b, Param c, Param d, Param e, Param f, Param g,
          Param h)
    => QueryParams (a,b,c,d,e,f,g,h) where
    renderParams (a,b,c,d,e,f,g,h) =
        [render a, render b, render c, render d, render e, render f, render g,
         render h]

instance (Param a, Param b, Param c, Param d, Param e, Param f, Param g,
          Param h, Param i)
    => QueryParams (a,b,c,d,e,f,g,h,i) where
    renderParams (a,b,c,d,e,f,g,h,i) =
        [render a, render b, render c, render d, render e, render f, render g,
         render h, render i]

instance (Param a, Param b, Param c, Param d, Param e, Param f, Param g,
          Param h, Param i, Param j)
    => QueryParams (a,b,c,d,e,f,g,h,i,j) where
    renderParams (a,b,c,d,e,f,g,h,i,j) =
        [render a, render b, render c, render d, render e, render f, render g,
         render h, render i, render j]

instance (Param a) => QueryParams [a] where
    renderParams = map render



-- | A collection type that can be converted from a list of strings.
--
-- Instances should use the 'convert' method of the 'Result' class
-- to perform conversion of each element of the collection.
--
-- This example instance demonstrates how to convert a two-column row
-- into a Haskell pair. Each field in the metadata is paired up with
-- each value from the row, and the two are passed to 'convert'.
--
-- @
-- instance ('Result' a, 'Result' b) => 'QueryResults' (a,b) where
--     'convertResults' [fa,fb] [va,vb] = (a,b)
--         where !a = 'convert' fa va
--               !b = 'convert' fb vb
--     'convertResults' fs vs  = 'convertError' fs vs
-- @
--
-- Notice that this instance evaluates each element to WHNF before
-- constructing the pair. By doing this, we guarantee two important
-- properties:
--
-- * Keep resource usage under control by preventing the construction
--   of potentially long-lived thunks.
--
-- * Ensure that any 'ResultError' that might arise is thrown
--   immediately, rather than some place later in application code
--   that cannot handle it.
--
-- You can also declare Haskell types of your own to be instances of
-- 'QueryResults'.
--
-- @
-- data User { firstName :: String, lastName :: String }

-- instance 'QueryResults' User where
--    'convertResults' [fa,fb] [va,vb] = User a b
--        where !a = 'convert' fa va
--              !b = 'convert' fb vb
--    'convertResults' fs vs  = 'convertError' fs vs
-- @

class QueryResults a where
    convertResults :: [Field] -> [Maybe ByteString] -> a
    -- ^ Convert values from a row into a Haskell collection.
    --
    -- This function will throw a 'ResultError' if conversion of the
    -- collection fails.

instance (Result a) => QueryResults (Only a) where
    convertResults [fa] [va] = Only a
        where a = convert fa va
    convertResults fs vs  = convertError fs vs 1

instance (Result a, Result b) => QueryResults (a,b) where
    convertResults [fa,fb] [va,vb] = (a,b)
        where a = convert fa va; b = convert fb vb
    convertResults fs vs  = convertError fs vs 2

instance (Result a, Result b, Result c) => QueryResults (a,b,c) where
    convertResults [fa,fb,fc] [va,vb,vc] = (a,b,c)
        where a = convert fa va; b = convert fb vb; c = convert fc vc
    convertResults fs vs  = convertError fs vs 3

instance (Result a, Result b, Result c, Result d) =>
    QueryResults (a,b,c,d) where
    convertResults [fa,fb,fc,fd] [va,vb,vc,vd] = (a,b,c,d)
        where a = convert fa va; b = convert fb vb; c = convert fc vc
              d = convert fd vd
    convertResults fs vs  = convertError fs vs 4

instance (Result a, Result b, Result c, Result d, Result e) =>
    QueryResults (a,b,c,d,e) where
    convertResults [fa,fb,fc,fd,fe] [va,vb,vc,vd,ve] = (a,b,c,d,e)
        where a = convert fa va; b = convert fb vb; c = convert fc vc
              d = convert fd vd; e = convert fe ve
    convertResults fs vs  = convertError fs vs 5

instance (Result a, Result b, Result c, Result d, Result e, Result f) =>
    QueryResults (a,b,c,d,e,f) where
    convertResults [fa,fb,fc,fd,fe,ff] [va,vb,vc,vd,ve,vf] = (a,b,c,d,e,f)
        where a = convert fa va; b = convert fb vb; c = convert fc vc
              d = convert fd vd; e = convert fe ve; f = convert ff vf
    convertResults fs vs  = convertError fs vs 6

instance (Result a, Result b, Result c, Result d, Result e, Result f,
          Result g) =>
    QueryResults (a,b,c,d,e,f,g) where
    convertResults [fa,fb,fc,fd,fe,ff,fg] [va,vb,vc,vd,ve,vf,vg] =
        (a,b,c,d,e,f,g)
        where a = convert fa va; b = convert fb vb; c = convert fc vc
              d = convert fd vd; e = convert fe ve; f = convert ff vf
              g = convert fg vg
    convertResults fs vs  = convertError fs vs 7

instance (Result a, Result b, Result c, Result d, Result e, Result f,
          Result g, Result h) =>
    QueryResults (a,b,c,d,e,f,g,h) where
    convertResults [fa,fb,fc,fd,fe,ff,fg,fh] [va,vb,vc,vd,ve,vf,vg,vh] =
        (a,b,c,d,e,f,g,h)
        where a = convert fa va; b = convert fb vb; c = convert fc vc
              d = convert fd vd; e = convert fe ve; f = convert ff vf
              g = convert fg vg; h = convert fh vh
    convertResults fs vs  = convertError fs vs 8

instance (Result a, Result b, Result c, Result d, Result e, Result f,
          Result g, Result h, Result i) =>
    QueryResults (a,b,c,d,e,f,g,h,i) where
    convertResults [fa,fb,fc,fd,fe,ff,fg,fh,fi] [va,vb,vc,vd,ve,vf,vg,vh,vi] =
        (a,b,c,d,e,f,g,h,i)
        where a = convert fa va; b = convert fb vb; c = convert fc vc
              d = convert fd vd; e = convert fe ve; f = convert ff vf
              g = convert fg vg; h = convert fh vh; i = convert fi vi
    convertResults fs vs  = convertError fs vs 9

instance (Result a, Result b, Result c, Result d, Result e, Result f,
          Result g, Result h, Result i, Result j) =>
    QueryResults (a,b,c,d,e,f,g,h,i,j) where
    convertResults [fa,fb,fc,fd,fe,ff,fg,fh,fi,fj]
                   [va,vb,vc,vd,ve,vf,vg,vh,vi,vj] =
        (a,b,c,d,e,f,g,h,i,j)
        where a = convert fa va; b = convert fb vb; c = convert fc vc
              d = convert fd vd; e = convert fe ve; f = convert ff vf
              g = convert fg vg; h = convert fh vh; i = convert fi vi
              j = convert fj vj
    convertResults fs vs  = convertError fs vs 10

-- | Throw a 'ConversionFailed' exception, indicating a mismatch
-- between the number of columns in the 'Field' and row, and the
-- number in the collection to be converted to.
convertError :: [Field] -> [Maybe ByteString] -> Int -> a
convertError fs vs n = throw $ ConversionFailed
    (show (length fs) ++ " values: " ++ show (zip (map fieldType fs)
                                                  (map (fmap ellipsis) vs)))
    (show n ++ " slots in target type")
    "mismatch between number of columns to convert and number in target type"

ellipsis :: ByteString -> ByteString
ellipsis bs
    | B.length bs > 15 = B.take 10 bs `B.append` "[...]"
    | otherwise        = bs



-- | Exception thrown if conversion from a SQL value to a Haskell
-- value fails.
data ResultError = Incompatible { errSQLType :: String
                                , errHaskellType :: String
                                , errMessage :: String }
                 -- ^ The SQL and Haskell types are not compatible.
                 | UnexpectedNull { errSQLType :: String
                                  , errHaskellType :: String
                                  , errMessage :: String }
                 -- ^ A SQL @NULL@ was encountered when the Haskell
                 -- type did not permit it.
                 | ConversionFailed { errSQLType :: String
                                    , errHaskellType :: String
                                    , errMessage :: String }
                 -- ^ The SQL value could not be parsed, or could not
                 -- be represented as a valid Haskell value, or an
                 -- unexpected low-level error occurred (e.g. mismatch
                 -- between metadata and actual data in a row).
                   deriving (Eq, Show, Typeable)

instance Exception ResultError

-- | A type that may be converted from a SQL type.
class Result a where
    convert :: Field -> Maybe ByteString -> a
    -- ^ Convert a SQL value to a Haskell value.
    --
    -- Throws a 'ResultError' if conversion fails.

instance (Result a) => Result (Maybe a) where
    convert _ Nothing = Nothing
    convert f bs      = Just (convert f bs)

instance Result Bool where
    convert _ (Just t)
      | str == "t" = True
      | str == "f" = False
     where str = B8.unpack t
    convert f _ = conversionFailed f "Bool" "could not parse"

instance Result Int16 where
    convert = atto ok16 $ signed decimal

instance Result Int32 where
    convert = atto ok32 $ signed decimal

instance Result Int where
    convert = atto okWord $ signed decimal

instance Result Int64 where
    convert = atto ok64 $ signed decimal

instance Result Integer where
    convert = atto ok64 $ signed decimal

instance Result Word16 where
    convert = atto ok16 decimal

instance Result Word32 where
    convert = atto ok32 decimal

instance Result Word where
    convert = atto okWord decimal

instance Result Word64 where
    convert = atto ok64 decimal

instance Result Float where
    convert = atto ok ((fromRational . toRational) <$> double)
        where ok = mkCompats [Real,Short,Long]

instance Result Double where
    convert = atto ok double
        where ok = mkCompats [Real,DoublePrecision,Short,Long]

instance Result (Ratio Integer) where
    convert = atto ok rational
        where ok = mkCompats [Decimal,Numeric,Real,DoublePrecision]

instance Result SB.ByteString where
    convert f = doConvert f okText $ id

instance Result LB.ByteString where
    convert f = LB.fromChunks . (:[]) . convert f

instance Result ST.Text where
    convert f | isText f  = doConvert f okText $ ST.decodeUtf8
              | otherwise = incompatible f (typeOf ST.empty)
                            "attempt to mix binary and text"

instance Result LT.Text where
    convert f = LT.fromStrict . convert f

instance Result [Char] where
    convert f = ST.unpack . convert f

instance Result LocalTime where
    convert f = doConvert f ok $ \bs ->
                case parseLocalTime (B8.unpack bs) of
                  Just t -> t
                  Nothing -> conversionFailed f "UTCTime" "could not parse"
        where ok = mkCompats [TimestampWithZone]

parseLocalTime :: String -> Maybe LocalTime
parseLocalTime s =
  parseTime defaultTimeLocale "%F %T%Q" s <|>
  parseTime defaultTimeLocale "%F %T%Q%z" (s ++ "00")

instance Result ZonedTime where
    convert f = doConvert f ok $ \bs ->
                case parseZonedTime (B8.unpack bs) of
                  Just t -> t
                  Nothing -> conversionFailed f "UTCTime" "could not parse"
        where ok = mkCompats [TimestampWithZone]

parseZonedTime :: String -> Maybe ZonedTime
parseZonedTime s =
  parseTime defaultTimeLocale "%F %T%Q%z" (s ++ "00")

instance Result UTCTime where
    convert f = doConvert f ok $ \bs ->
                case parseTime defaultTimeLocale "%F %T%Q" (B8.unpack bs) of
                  Just t -> t
                  Nothing -> conversionFailed f "UTCTime" "could not parse"
        where ok = mkCompats [Timestamp]

instance Result Day where
    convert f = flip (atto ok) f $ date
        where ok = mkCompats [Date]
              date = fromGregorian <$> (decimal <* char '-')
                                   <*> (decimal <* char '-')
                                   <*> decimal

instance Result TimeOfDay where
    convert f = flip (atto ok) f $ do
                hours <- decimal <* char ':'
                mins <- decimal <* char ':'
                secs <- decimal :: Parser Int
                case makeTimeOfDayValid hours mins (fromIntegral secs) of
                  Just t -> return t
                  _      -> conversionFailed f "TimeOfDay" "could not parse"
        where ok = mkCompats [Time]

isText :: Field -> Bool
isText f = fieldFormatCode f == TextCode

newtype Compat = Compat Word32

mkCompats :: [Type] -> Compat
mkCompats = foldl' f (Compat 0) . map mkCompat
  where f (Compat a) (Compat b) = Compat (a .|. b)

mkCompat :: Type -> Compat
mkCompat = Compat . shiftL 1 . fromEnum

compat :: Compat -> Compat -> Bool
compat (Compat a) (Compat b) = a .&. b /= 0

okText, ok16, ok32, ok64, okWord :: Compat
okText = mkCompats [CharVarying,Characters,Text]
ok16 = mkCompats [Short]
ok32 = mkCompats [Short,Long]
ok64 = mkCompats [Short,Long,LongLong]

okWord = ok64

doConvert :: (Typeable a) =>
             Field -> Compat -> (ByteString -> a) -> Maybe ByteString -> a
doConvert f types cvt (Just bs)
    | mkCompat (fieldType f) `compat` types = cvt bs
    | otherwise = incompatible f (typeOf (cvt undefined)) "types incompatible"
doConvert f _ cvt _ = throw $ UnexpectedNull (show (fieldType f))
                              (show (typeOf (cvt undefined))) ""

incompatible :: Field -> TypeRep -> String -> a
incompatible f r = throw . Incompatible (show (fieldType f)) (show r)

conversionFailed :: Field -> String -> String -> a
conversionFailed f s = throw . ConversionFailed (show (fieldType f)) s

atto :: (Typeable a) => Compat -> Parser a -> Field -> Maybe ByteString -> a
atto types p0 f = doConvert f types $ go (error "atto") p0
  where
    go :: (Typeable a) => a -> Parser a -> ByteString -> a
    go dummy p s =
        case parseOnly p s of
          Left err -> conversionFailed f (show (typeOf dummy)) err
          Right v  -> v


-- | A placeholder for the SQL @NULL@ value.
data Null = Null
          deriving (Read, Show, Typeable)

instance Eq Null where
    _ == _ = False
    _ /= _ = False

-- | A query string. This type is intended to make it difficult to
-- construct a SQL query by concatenating string fragments, as that is
-- an extremely common way to accidentally introduce SQL injection
-- vulnerabilities into an application.
--
-- This type is an instance of 'IsString', so the easiest way to
-- construct a query is to enable the @OverloadedStrings@ language
-- extension and then simply write the query in double quotes.
--
-- >
-- > import Database.MySQL.Simple
-- >
-- > q :: Query
-- > q = "select ?"
--
-- The underlying type is a 'ByteString', and literal Haskell strings
-- that contain Unicode characters will be correctly transformed to
-- UTF-8.
newtype Query = Query {
      fromQuery :: ByteString
    } deriving (Eq, Ord, Typeable)

instance Show Query where
    show = show . fromQuery

instance Read Query where
    readsPrec i = fmap (first Query) . readsPrec i

instance IsString Query where
    fromString = Query . toByteString . Utf8.fromString

instance Monoid Query where
    mempty = Query B.empty
    mappend (Query a) (Query b) = Query (B.append a b)

-- | A single-value \"collection\".
--
-- This is useful if you need to supply a single parameter to a SQL
-- query, or extract a single column from a SQL result.
--
-- Parameter example:
--
-- @query c \"select x from scores where x > ?\" ('Only' (42::Int))@
--
-- Result example:
--
-- @xs <- query_ c \"select id from users\"
--forM_ xs $ \\('Only' id) -> {- ... -}@
newtype Only a = Only {
      fromOnly :: a
    } deriving (Eq, Ord, Read, Show, Typeable, Functor)

-- | Wrap a list of values for use in an @IN@ clause.  Replaces a
-- single \"@?@\" character with a parenthesized list of rendered
-- values.
--
-- Example:
--
-- > query c "select * from whatever where id in ?" (In [3,4,5])
newtype In a = In a
    deriving (Eq, Ord, Read, Show, Typeable, Functor)

-- | Wrap a mostly-binary string to be escaped in hexadecimal.
newtype Binary a = Binary a
    deriving (Eq, Ord, Read, Show, Typeable, Functor)

-}

