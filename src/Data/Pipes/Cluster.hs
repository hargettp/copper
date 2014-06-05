{- |
This module defines a set of pipes useful for distributing stream processing across
a cluster of nodes all running the same pipe.
-}

module Data.Pipes.Cluster (
  -- * Basic primitives
  balance,
  broadcast,
  gather,
  ) where

-- Local imports  
import Data.Pipes.Core
import Data.Pipes.Network
  
-- External imports

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{- |
Attempt to distribute the incoming events from all running instances of this 'Pipe'
across all available instances, to share the workload.  

Any single 'Event' will be processed by only one node.
-}
balance :: Server -> String -> Pipe i i
balance _ _ = pass

{- |
Copy every incoming 'Event' from all running instances of this 'Pipe' to
every other running instance, so that all instances process the same events.  

Any single 'Event' will be processed by all nodes running the 'Pipe'.
-}
broadcast :: Server -> String -> Pipe i i
broadcast _ _ = pass

{- |
Localize execution of the pipe so that all incoming event from all running instances
of the 'Pipe' are delivered to exactly one node.  Because only one node receives
all events, effectively all other running instances of the pipe process no events,
and no downstream pipes receive events, unless there is another 'brodcast' or 
'balance' pipe.

Any single 'Event' will be processed by a single node.
-}
gather :: Server -> String -> Pipe i i
gather _ _ = pass
