{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE DefaultSignatures #-}
{-# LANGUAGE DeriveDataTypeable #-}

{- |
Servers pass messages between one another to coordinate work.
-}
module Data.Pipes.Message (
  Message(..),
  sendMessage,
  recvMessage
  ) where

-- Local imports

-- External imports

import Control.Exception (
  Exception,
  throw
  )

import Data.ByteString as B

import Data.Serialize (
  Serialize,
  decode,
  encode,
  get,
  getWord16be,
  put,
  putWord16be
  )
    
import Data.Typeable (  
  Typeable
  )
  
import Data.Word (  
  Word32
  )
  
import GHC.Generics (Generic)

import Network.Socket hiding (send, sendTo, recv, recvFrom)
import Network.Socket.ByteString

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

{- |
A 'Message' is a unit of information exchange between members of a cluster.
-}
data Message = 
  -- | Identify the cluster name and server name of the sending server
  Identify String String
  -- | Request work for the indicated server and named pipe
  | RequestWork String String
  -- | Receive work for the indicated named pipe
  | Work String B.ByteString
  -- | Request to join the cluster with the given server name
  | Join String String
  -- | Announce the following servers as members of the named cluster
  | Members String [String] deriving (Show,Generic)
                                     
instance Serialize Message

data Version = Version {
  major :: Int,
  minor :: Int
  } deriving (Show)
             
instance Serialize Version where
  get = do
     mjr <- getWord16be
     mnr <- getWord16be
     return $ Version (fromIntegral mjr) (fromIntegral mnr)
  put v = do
    putWord16be $ fromIntegral $ major v
    putWord16be $ fromIntegral $ minor v

protocolVersion :: Version               
protocolVersion = Version 1 0

{- |
Return the version for a given message.  Note that at the moment
all messages have the same version, but this could change in the future.
-}
messageVersion :: Message -> Version
messageVersion _ = protocolVersion

{- |
Send a single 'Message' in its entirety over the provided 'Socket'.
-}
sendMessage :: Socket -> Message -> IO ()
sendMessage s msg = do 
  sendMessageVersion
  sendMessageBytes where
    sendMessageVersion = sendAll s $ encode $ messageVersion msg
    sendMessageBytes = do
      sendAll s $ encode messageLength
      sendAll s $ messageBytes
    messageLength :: Word32
    messageLength = fromIntegral $ B.length messageBytes
    messageBytes = encode msg  

{- |
Receive the specified number of bytes from the socket,
otherwise throwing an exception.

-}
recvAll :: Socket -> Int -> IO ByteString
recvAll s n = do
  bytes <- recv s n
  if (B.length bytes) < n
    then recvMore s n bytes
    else return bytes where
    recvMore s1 n1 bytes = do
      more <- recvAll s1 (n1 - (B.length bytes))
      return $ B.append bytes more
      
data UndecodableError =  UndecodableError deriving (Show,Typeable)

instance Exception UndecodableError

recvDecAll :: (Serialize m) => Socket -> Int -> IO m
recvDecAll s n = do
  bytes <- recvAll s n
  return $ either failure success (decode bytes) where
    failure _ = throw UndecodableError
    success msg = msg

data UnknownVersionError = UnknownVersionError Version deriving (Show,Typeable)

instance Exception UnknownVersionError

{- |
Receive a single complete 'Message' over the 'Socket'.
-}
recvMessage :: Socket -> IO Message
recvMessage s = do
  version <- recvDecAll s 4
  if isAcceptableVersion version
    then recvBytes
    else throw $ UnknownVersionError version where 
    
    isAcceptableVersion :: Version -> Bool
    isAcceptableVersion v = (major v == (major protocolVersion)) 
                            && (minor v == (minor protocolVersion))
                            
    recvBytes = do
      n <- recvDecAll s 4
      recvDecAll s n
      