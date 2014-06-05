{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE DefaultSignatures #-}

{- |
A library of useful pipes
-}
module Data.Pipes.Library (
  count,
  echo,
  fileLines,
  final,
  fromList,
  pmap,
  format
  ) where

-- Local imports  
import Data.Pipes.Core
  
-- External imports

import qualified Data.ByteString as B

import Data.Text (
  Text
  )

import Data.Text.Encoding (  
  decodeUtf8
  )

import Control.Concurrent.STM (
  atomically,
  newTVar,
  readTVar,
  writeTVar
  )

import System.IO (
  hIsEOF,
  withFile,
  IOMode(..)
  )
  
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{- |
Emit each value in the supplied list downstream.  This processor takes only a single @()@
as input.  Once the values in the list have been emitted, the stream is closed.
-}
fromList :: [a] -> Pipe () a
fromList l = pipe (\ev em -> 
               case ev of 
                 EOS -> em EOS
                 _ -> mapM_ em $ map (Event) l)
             
{- | A simplified constructor for pipes that simply map an input value to a corresponding
output value.
-}
pmap :: (i -> o) -> Pipe i o             
pmap fn = process (\i fno ->
                    fno $ fn i)
             
{- |
Echo the input string to stdout, and then emit it downstream.

-}
echo :: Pipe String String
echo = process (\i fno -> do 
                   fno i
                   putStrLn i)

{- |
Given a 'Showable' as input, render it as a string and emit it to output
-}
format :: (Show a) => Pipe a String       
format = pmap show
         
{- |
Count each value received on input, and emit the count and the value
in a tuple as output.
-}
count :: Pipe i (i,Int)        
count = let incCounter c = atomically $ do
              val <- readTVar c
              let newVal = val + 1
                in do
                writeTVar c $! newVal
                return newVal
            countEvents _ EOS emci = emci EOS
            countEvents c (Event i) emci = do
              new <- incCounter c
              emci $ Event (i,new)
        in statefulPipe (atomically $ newTVar 0) countEvents
        
{- |
Given each 'FilePath' received as input, open each such file and emit
the lines of those files to the output.
-}
fileLines :: Pipe FilePath Text
fileLines = pipe (\evt emt ->
                     case evt of
                       EOS -> emt EOS
                       Event fp -> withFile fp ReadMode $ emitLines emt
                   ) where
  emitLines emt h = do
    eof <- hIsEOF h
    if eof 
      then return () 
      else emitLine emt h
  emitLine emt h = do
    line <- B.hGetLine h
    _ <- emt $ (Event . decodeUtf8) line
    emitLines emt h
        
{- |
Emit the last value seen before the stream is closed.
-}
final :: i -> Pipe i i
final initial = let saveEvent ref EOS emo = do
                      val <- atomically $ readTVar ref
                      emo $ Event val
                      emo EOS
                    saveEvent ref (Event i) _ = do
                      atomically $ writeTVar ref $! i
                      return ()
                in statefulPipe (atomically $ newTVar initial) saveEvent
