{-|
A general purpose library for efficient, concurrent stream processing.

Consumers of this library can simply import "Data.Pipes", as the definitions in submodules
are automatically re-exported.

Note that as 'Pipe' is a 'Category', consumers may choose to use 'Control.Category.>>>' when chaining
pipes together, which this module re-exports for convenience. Alternatively,
this module does provide 'Data.Pipes.Core.=&=' which is a synonym for 'Control.Category.>>>'.

-}
module Data.Pipes ( 
  -- | Definitions of fundamental 'Pipe' behavior.
  module Data.Pipes.Core,
  -- | A library of useful 'Pipe' definitions for a variety of tasks.
  module Data.Pipes.Library,
  -- | Pipe definitions supporting load distribution across a cluster of nodes
  module Data.Pipes.Cluster,
  module Data.Pipes.Network
  ) where

import Data.Pipes.Core
import Data.Pipes.Library
import Data.Pipes.Network
import Data.Pipes.Cluster