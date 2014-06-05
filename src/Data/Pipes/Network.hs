{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE ExistentialQuantification #-}
{-# LANGUAGE DefaultSignatures #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

{- |
This module defines the networking layer of pipes, supporting distributed execution
of pipe processors across a clust of nodes all running the same pipe program.

Each program intendeding to participate in a cluster should start a 'Server',
specifying the name of the cluster to join and using a unique name for the server
to differentiate it from other nodes in the cluster.  

Once the server is started, the server will run two listeners, one UDP and one TCP.
The UDP listener monitors for announcements of cluster membership by other nodes,
and the TCP listener handles requests for work.

-}
module Data.Pipes.Network (
  -- * Servers
  Server,
  newServer,
  startServer,
  stopServer,
  -- * Readers
  Reader,
  reader,
  tryReadEvents,
  addReader,
  removeReader
  ) where

-- Local imports  
import Data.Pipes.Core
import Data.Pipes.Message
  
-- External imports
import Control.Concurrent
import Control.Concurrent.STM (
  TChan,
  TVar,
  atomically,
  modifyTVar',
  newTVar,
  readTVar,
  newTChan,
  readTChan,
  writeTChan,
  tryReadTChan
  )
import Control.Exception

import qualified Data.Map as M

import Data.Serialize 
  (Serialize,
   decode,
   encode,
   get,
   getWord16be,
   put,
   putWord16be)

import GHC.Generics (Generic)

import Network hiding (sendTo,recvFrom,accept)
import Network.Socket (
  accept,
  aNY_PORT,
  close,
  getPeerName,
  PortNumber(PortNum)
  )
import Network.Socket.ByteString (sendTo,recvFrom)
import Network.BSD (getHostName)
import Network.Multicast

--------------------------------------------------------------------------------
--------------------------------------------------------------------------------

defaultServerPort :: PortNumber
defaultServerPort = 7133

data ServerAddress = ServerAddress {
  serverHost :: String,
  serverPort :: PortNumber
  } deriving (Eq,Ord,Show,Generic)
             
instance Serialize ServerAddress

{- |
A reader contains a function that can read events: the function either returns an 'Event'
wrapped in 'Just', or 'Nothing' if no event is available.
-}
data Reader = forall e. (Serialize e) => Reader {
  -- | Return an event 'Event' wrapped in 'Maybe'
  tryReadEvents :: IO (Maybe (Event e))
  }
                
{- |
Given a 'TChan', constructs a reader for polling it.
-}
reader :: (Serialize e) => TChan (Event e) -> Reader
reader c = Reader {
  tryReadEvents = atomically $ do
     tryReadTChan c
  }
           
{- |
Add a 'Reader' to the list of readers known to the 'Server'. Each reader should have
a unique name. Such readers can be polled from across the by other servers in an attempt offload work.
-}
addReader :: Server -> String -> Reader -> IO ()
addReader server name rdr = atomically $ 
                            modifyTVar' (serverReaders server) (\oldReaders -> M.insert name rdr oldReaders)
                            
{- |
Remove a 'Reader' from the list of readers known to the 'Server', given the name of the 'Reader'.
-}
removeReader :: Server -> String -> IO ()                            
removeReader server name = atomically $
                           modifyTVar' (serverReaders server) (\oldReaders -> M.delete name oldReaders)

{- |
Servers listen for work coordination requests from other servers.
-}
data Server = Server {
  clusterName :: String,
  serverName :: String,
  serverAddress :: ServerAddress,
  clusterDiscovery :: Discovery,
  serverListenerThreadId :: Maybe ThreadId,
  serverReaders :: TVar (M.Map String Reader),
  serverConnections :: TVar (M.Map ServerAddress Connection)
  }
              
{- |
Given a cluster name and a unique server name, return a new server instance
-}
newServer :: String -> String -> IO Server               
newServer cluster server = do 
  readers <- atomically $ newTVar M.empty
  connections <- atomically $ newTVar M.empty
  hostname <- getHostName
  discovery <- newDiscovery
  return Server {
    clusterName = cluster,
    serverName = server,
    serverAddress = ServerAddress {
      serverHost = hostname,
      serverPort = defaultServerPort
      },
    clusterDiscovery = discovery,
    serverListenerThreadId = Nothing,
    serverReaders = readers,
    serverConnections = connections
    }
                 
{- |
Start the server, making it ready to run pipes or offload work from other servers
-}
startServer :: Server -> IO Server
startServer server = do
  startServerListener server
    >>= startDiscoveryListener
    >>= startDiscoveryAnnouncement 
    
{- |    
Stop the server, preventing it from offloading any more work from other servers
-}
stopServer :: Server -> IO Server    
stopServer server = do
  stopDiscoveryAnnouncement server
  >>= stopDiscoveryListener
  >>= stopServerListener
  >>= stopServerConnections
  
stopServerConnections :: Server -> IO Server
stopServerConnections server = do
  connections <- atomically $ do
    m <- readTVar $ serverConnections server
    return $ M.elems m
  mapM_ stopConnection connections
  return server
  
--------------------------------------------------------------------------------
--                                     
-- discovery
--                                     
--------------------------------------------------------------------------------
defaultDiscoveryAddress :: String
defaultDiscoveryAddress = "239.117.0.1"

defaultDiscoveryPort :: PortNumber
defaultDiscoveryPort = 7122

data Announcement = Announcement {
  announcementIsInitial :: Bool,
  announcementClusterName :: String,
  announcementServerName :: String,
  announcementServerAddress :: ServerAddress
  } deriving (Show,Generic)
             
instance Serialize Announcement             

instance Serialize PortNumber where
  get = do
    val <-  getWord16be
    return $ PortNum $ val
  put p = let PortNum port = p 
          in putWord16be port
             
serverAnnouncement :: Server -> Bool -> Announcement  
serverAnnouncement server first = Announcement {
  announcementIsInitial = first,
  announcementClusterName = (clusterName server),
  announcementServerName = (serverName server),
  announcementServerAddress = serverAddress server
  }

data Discovery = Discovery {  
  discoveryAddress :: String,
  discoveryPort :: PortNumber,
  discoveryClusterMembers :: TVar (M.Map String ServerAddress),
  discoveryListenerThreadId :: Maybe ThreadId,
  discoveryAnnouncementThreadId :: Maybe ThreadId
  }

newDiscovery :: IO Discovery 
newDiscovery = atomically $ do
  members <- newTVar M.empty
  return Discovery {
  discoveryAddress = defaultDiscoveryAddress,
  discoveryPort = defaultDiscoveryPort,
  discoveryClusterMembers = members,
  discoveryListenerThreadId = Nothing,
  discoveryAnnouncementThreadId = Nothing
  }
    
startDiscoveryListener :: Server -> IO Server
startDiscoveryListener server = withSocketsDo $ do
        sock <- multicastReceiver defaultDiscoveryAddress defaultDiscoveryPort
        let loop = do
              (msg, addr) <- recvFrom sock 1024
              _ <- forkIO $ socketHandler (msg,addr)
              loop
            socketHandler (bytes, _ ) = let msg :: Either String Announcement
                                            msg = decode bytes 
                                            Right announcement = msg
                                         in if (announcementClusterName announcement) == (clusterName server) 
                                            then discover announcement
                                            else return ()
            discover announcement = do 
              members <- atomically $ do
                modifyTVar' (discoveryClusterMembers discovery) 
                  (\members -> M.insert (announcementServerName announcement)
                               (announcementServerAddress announcement)
                               members )
                readTVar $ discoveryClusterMembers discovery
              print members
            discovery = clusterDiscovery server
          in do
          tid <- forkIO $ finally loop (sClose sock)
          return server {clusterDiscovery = discovery {discoveryListenerThreadId = Just tid}}
          
stopDiscoveryListener :: Server -> IO Server
stopDiscoveryListener server = let stop (Just tid) = do
                                     killThread tid
                                     return $ server {
                                       clusterDiscovery = discovery{
                                          discoveryListenerThreadId = Nothing}}
                                   stop Nothing = return server
                                   discovery = clusterDiscovery server
                               in do
                                 stop $ discoveryListenerThreadId $ clusterDiscovery server

initialAnnouncementDelay :: Int
initialAnnouncementDelay = 1000000

announcementDelay :: Int
announcementDelay = 5000000

startDiscoveryAnnouncement :: Server -> IO Server
startDiscoveryAnnouncement server = withSocketsDo $ do
  (sock,addr) <- multicastSender defaultDiscoveryAddress defaultDiscoveryPort
  let  first = do
         _ <- sendTo sock (encode $ serverAnnouncement server True) addr
         threadDelay initialAnnouncementDelay
       loop = do
         _ <- sendTo sock (encode $ serverAnnouncement server False) addr
         threadDelay announcementDelay
         loop 
       discovery = clusterDiscovery server
       announce = do
         first
         loop
    in do
       tid <- forkIO $ finally announce (sClose sock)
       return server {clusterDiscovery = discovery {discoveryAnnouncementThreadId = Just tid}}

stopDiscoveryAnnouncement :: Server -> IO Server
stopDiscoveryAnnouncement server = let stop (Just tid) = do
                                         killThread tid
                                         return $ server {
                                           clusterDiscovery = discovery {
                                              discoveryAnnouncementThreadId = Nothing
                                              }}
                                       stop Nothing = return server
                                       discovery = clusterDiscovery server
                                   in do
                                     stop $ discoveryAnnouncementThreadId $ clusterDiscovery server

--------------------------------------------------------------------------------
--                                     
-- server listener
--                                     
--------------------------------------------------------------------------------

startServerListener :: Server -> IO Server
startServerListener server = do
  sock <- listen
  PortNumber port <- socketPort sock
  tid <- forkIO $ loop sock
  return server {
    serverListenerThreadId = Just tid,
    serverAddress = (serverAddress server) {serverPort = port}} 
  where
    listen = withSocketsDo $ do
      sock <- listenOn $ PortNumber aNY_PORT
      return sock
    loop sock = do  
      conn <- accept sock
      _ <- forkIO $  handleConnection conn
      loop sock
    handleConnection (clientSocket,_) = do
      connection <- newConnection server clientSocket
      startConnection connection
      return ()

stopServerListener :: Server -> IO Server
stopServerListener server = stop $ serverListenerThreadId server 
                            where 
                              stop (Just tid) = do
                                killThread tid
                                return $ server {serverListenerThreadId = Nothing}
                              stop Nothing = return server
  
--------------------------------------------------------------------------------
--                                     
-- connections
--                                     
--------------------------------------------------------------------------------
                              
data Connection = Connection {
  connectionServer :: Server,
  connectionSocket :: Socket,
  connectionOutgoing :: TChan Message,
  connectionShutdown :: TChan ()
  }
                 
newConnection :: Server -> Socket -> IO Connection                  
newConnection server socket = do
  outgoing <- atomically $ newTChan
  shutdown <- atomically $ newTChan
  return Connection {
    connectionServer = server,
    connectionSocket = socket,
    connectionOutgoing = outgoing,
    connectionShutdown = shutdown
    }
        
startConnection :: Connection -> IO ()    
startConnection conn = do
  outgoingThread <- forkIO $ runOutgoingConnection server socket shutdown outgoing
  incomingThread <- forkIO $ runIncomingConnection server socket shutdown
  _ <- forkIO $ do
    _ <- atomically $ readTChan (connectionShutdown conn)
    killThread outgoingThread
    killThread incomingThread
    close socket
  return ()
  where
    server = connectionServer conn
    socket = connectionSocket conn
    outgoing = connectionOutgoing conn
    shutdown = atomically $ writeTChan (connectionShutdown conn) ()
      
runIncomingConnection :: Server -> Socket -> IO () -> IO ()
runIncomingConnection _ socket shutdown = finally handleGreeting shutdown
  where
    
    handleGreeting = do
      msg <- recvMessage socket
      case msg of
        Identify cName sName -> do
          addr <- getPeerName socket
          print $ "Found " ++ sName ++ " of " ++ cName ++ " at " ++ (show addr)
          handleMessages
        _ -> close socket
        
    handleMessages = do
      msg <- recvMessage socket
      handleMessage msg
      handleMessages
      
    handleMessage msg = print msg
          
runOutgoingConnection :: Server -> Socket -> IO () -> TChan Message -> IO ()
runOutgoingConnection server socket shutdown msgs = finally sendGreeting shutdown
  where
    sendGreeting = do
      sendMessage socket $ Identify (clusterName server) (serverName server)
      sendMessages
    sendMessages = do
      msg <- atomically $ readTChan msgs
      sendMessage socket msg
      
stopConnection :: Connection -> IO ()      
stopConnection conn = do
  atomically $ writeTChan (connectionShutdown conn) ()
