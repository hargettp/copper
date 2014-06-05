{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE ImpredicativeTypes #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DefaultSignatures #-}

{- |
The central concept is that of a 'Pipe', which has a typed input and a typed
output. Pipes pass input and output to one another via typed 'Event' objects, and
each pipe runs its own event 'Processor' in a separate lightweight thread created
with 'forkIO'. The mechanics of how pipes pass events to each other (or, more precisely, how
processors pass events to one another) is hidden in the concept of an 'Emitter': a function
whose sole job is to accept an 'Event' and dispatch it, typically to the next 'Processor'
in the chain.

Writers of new types of 'Processor' should take care that their implementation *always* terminates
by emitting 'EOS' as the last event before exiting its processing loop.  The 'EOS' event signals
to downstream 'Pipe' instances that processing should cease, and any event processing loops
can exit.  Failing to emit an 'EOS' but still exiting an event processing loop can cause downstream
processors to deadlock, as they continue to wait on an 'EOS' (or other 'Event') that will never come. 
The default implementation of an event loop in 'processEvents' attempts to be safe by wrapping the
event loop in an exception handler that simply emits an 'EOS' to close the stream.

One easy way to avoid these types of deadlock issues may be to use the 'process' construction function
as often as posible, as it will produce a pipe that automatically emits an 'EOS' downstream when
an 'EOS' is received as input Event.  The function supplied to 'process' will only be called
when the input 'Event' contains a value.

-}
module Data.Pipes.Core  ( 
  -- * Basic functional types
  Emitter,
  Processor,
  Transmitter,
  -- * Pipes and pipe construction
  Pipe,
  transmit,
  pipe,
  statefulPipe,
  process,
  -- * Building more complex pipes
  (=&=),
  (>>>),
  connect,
  pass,
  processEvents,
  -- * Running pipes
  runPipe,
  -- * Events
  Event(..),
  ) where

-- Local imports  
  
-- External imports
import Control.Category

import Control.Concurrent (
  forkIO
  )
  
import Control.Concurrent.STM (
  TChan,
  atomically,
  newTChan,
  readTChan,
  writeTChan,
  newEmptyTMVar,
  putTMVar,
  takeTMVar
  )
  
import Control.Exception (  
  catch,
  SomeException
  )
  
import Data.Serialize (
  Serialize
  )
  
import GHC.Generics (Generic)

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

-- | An event is a typed container of data passed from one 'Pipe' to another
data Event e = Event e | EOS deriving (Show,Generic)

instance (Serialize e) => Serialize (Event e)

-- | An emitter takes an 'Event' as input and passes it downstream
type Emitter o = Event o -> IO ()

{- | A processor accepts an 'Event' and an 'Emitter', and may (or may not)
invoke the emitter 1 or more times for the input event. A processor in stream
processing terms is analogous to function subroutine in imperative programming models:
while one invokes return in @return@ in imperative program languages to supply a value
back to the caller, in stream processing one can use an 'Emitter' to pass a value
to the next processor in the chain.
-}

type Processor i o = Event i -> Emitter o -> IO ()

{- | A transmitter produces a new 'Emitter', given a target output
 'Emitter', and is the type for the basic mechanism of connecting pipes together.
-}
type Transmitter i o = Emitter o -> IO (Emitter i)  


{- |
The central data type for processing events.
-}
data Pipe i o = Pipe {
  transmitEvents :: Transmitter i o
  }
                
instance Category Pipe where                
  id = pass
  (.) = (flip connect)
  
{- |
Read each 'Event' from the specified channel and allow the provided
'Processor' to handle it with the provided 'Emitter' for any output.
-}
processEvents :: TChan (Event i) -> Processor i o -> Emitter o -> IO ()
processEvents c p emo = do
  evt <- atomically $ readTChan c
  p evt emo
  case evt of
    EOS -> do
      return ()
    _ -> do
      processEvents c p emo
      
{- |   
Construct a 'Pipe' with a given 'Transmitter'.
-}
transmit :: Transmitter i o -> Pipe i o
transmit tm = Pipe {transmitEvents = tm}

{- |
Construct a pipe that runs the given processor in its own thread.
-}
pipe :: Processor i o -> Pipe i o
pipe pio = transmit (\emo -> do
                      ci <- atomically $ newTChan
                      _ <- forkIO $ catch (processEvents ci pio emo) (handle emo)
                      return (\evi -> do
                                atomically $ writeTChan ci $! evi)) where
  handle :: Emitter o -> SomeException -> IO ()
  handle emo e = do
    emo EOS
    putStrLn $ "Encountered error: "  ++ (show e)
             
      
{- |
Construct a pipe that starts with an initial state, and which passes
that state to a function which returns the processor to use for the pipe.  Useful
for stateful pipes that require some context preserved across events.
-}
statefulPipe :: IO s -> (s -> Processor i o) -> Pipe i o
statefulPipe initialState p = transmit (\emi -> do
               ci <- atomically newTChan
               state <- initialState
               _ <- forkIO $ processEvents ci (p state) emi
               return (\evi -> do
                          atomically $ writeTChan ci $! evi))
                             
{- | A simplified constructor for pipes that takes a function that does not      
use typed 'Event' objects in its input or output definitions. This may be useful in many
cases, as the returned 'Pipe' will automatically handle 'EOS', leaving the supplied
function with the task of simply performing whatever calculation on the input data that
is appropriate to produce output (if any).
-}
process :: (i -> (o -> IO () ) -> IO () ) -> Pipe i o
process fn = pipe (\evi emo ->
                       case evi of
                         EOS -> emo EOS
                         Event i -> fn i (\o -> emo $ Event o) )
             
{- | The identity or 'id' 'Pipe' that simply emits as output any value given to it as input.
-}
pass :: Pipe i i
pass = pipe (\evi emi -> emi evi)

{- | Joins two pipes together to form a longer chain by connecting the transmitters for each
pipe to one another.  This is also the basis for implementing '>>>' in the 'Pipe' 'Category' instance
-}
connect :: Pipe i o -> Pipe o p -> Pipe i p
connect pio pop = Pipe {
  transmitEvents = (\emp -> do
                        emo <- (transmitEvents pop) emp
                        emi <- (transmitEvents pio) emo
                        return emi)
  }

{- | At once a synonym for 'connect' and '>>>', this may be most useful for connecting
pipes together into longer chains in a visual style that may be less distracting than using 'connect'.
-}
(=&=) :: Pipe i o -> Pipe o p -> Pipe i p
(=&=) = connect

{- | Given an input value and a 'Pipe', run the pipe to completion.  Note that this function
does not itself produce any output values, regardless of the value of the @o@ parameter to the supplied
'Pipe' type.
-}
runPipe :: i -> Pipe i o -> IO ()
runPipe i p = do
  done <- atomically $ newEmptyTMVar
  let eo ev = do
        case ev of
          Event _ -> return ()
          EOS -> atomically $ putTMVar done ()
  ei <- (transmitEvents p) eo
  ei $ Event i
  ei EOS
  _ <- atomically $ takeTMVar done
  return ()

