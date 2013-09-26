import Diff
import System.Environment

main = do
  args <- getArgs
  a <- readFile (head args)
  b <- readFile (head $ tail args)
  let zz = diff (lines a) (lines b)
  putStr $ concatMap show zz
  