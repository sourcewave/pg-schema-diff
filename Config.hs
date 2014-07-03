#!/usr/bin/env runhaskell

module Config where

import System.IO
import System.Environment
import Control.Monad
import Text.Parsec
import Text.Printf (printf)
import Data.Either (rights)
import Data.Char (isSpace)
import Data.List (unionBy)
       
tkc x = spaces >> char x

optionx = do
  z <- notFollowedBy (char '#')
  a <- spaces
  k <- many1 (noneOf "= ")
  c <- tkc '='
  spaces
  v <- xstring <|> many1 (satisfy (\x -> not ((isSpace x) || x == '#' )))
  j <- comment 
  return (OptionSetting  k   v  (uncomment j))

data PostgresConf = Comment String | OptionSetting String String String

uncomment (Comment x) = x

instance Show PostgresConf where
--  show (Comment s) = if null s then s else "#" ++ s
  show (Comment s) = ""
  show (OptionSetting k v c) = k ++ " = " ++ (if any (`elem` v) " /#_$,-" then "'"++v++"'" else v) ++ (if null c then "" else "   #" ++ c) ++ "\n"

xstring = between (char '\'') (char '\'') (many (noneOf "'"))
comment = do { spaces; b <- try $ do { char '#';  a <- many (noneOf ""); eof; return a } <|> (eof >> return "") ; return (Comment b) }
optl = try comment <|> optionx

kwp = do { k <- spaces >> many1 (noneOf "= "); char '='; v <- many (noneOf ""); eof; return (OptionSetting k v "" ) }

opteq (OptionSetting x _ _ ) (OptionSetting y _ _) = x == y
opteq _ _ = False

main = do
  f:g <- getArgs
  let nn = map (parse kwp "Config") g

  h <- openFile f ReadMode
  a <- liftM lines $  hGetContents h
  let z = map (parse optl "Config") a
  mapM_ putStr (map show (unionBy opteq (rights nn) (rights z)))
