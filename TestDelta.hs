import Diff
import System.Environment

main = do
  args <- getArgs
  let a = head args
  let b = head $ tail args
  let zz = diff a b
  putStr $ concatMap show zz
  