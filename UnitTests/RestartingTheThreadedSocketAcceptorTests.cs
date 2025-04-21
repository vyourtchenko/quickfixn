using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using NUnit.Framework;
using QuickFix;

namespace UnitTests
{
    [TestFixture]
    [NonParallelizable]
    public class RestartingTheThreadedSocketAcceptorTests
    {

        private const string Host = "127.0.0.1";
        private const int AcceptPort = 55101;
        private const string StaticAcceptorCompID = "acc01";
        private const string StaticAcceptorCompID2 = "acc02";
        private const string ServerCompID = "dummy";
        // private ThreadedSocketAcceptor _acceptor = null;
        private const string FIXMessageEnd = @"\x0110=\d{3}\x01";
        private const string FIXMessageDelimit = @"(8=FIX|\A).*?(" + FIXMessageEnd + @"|\z)";
        // private Dictionary<string, SocketState> _sessions;
        // private HashSet<string> _loggedOnCompIDs;
        private Socket _socket01 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        private Socket _socket02 = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        private int _senderSequenceNumber = 1;

        public class TestApplication : IApplication
        {
            private Action<string, HashSet<string>> _logonNotify;
            private Action<string, HashSet<string>> _logoffNotify;
            private ThreadedSocketAcceptor _acceptor = null;
            private Dictionary<string, SocketState> _sessions;
            private HashSet<string> _loggedOnCompIDs;

            public TestApplication(Action<string, HashSet<string>> logonNotify, Action<string, HashSet<string>> logoffNotify)
            {
                _logonNotify = logonNotify;
                _logoffNotify = logoffNotify;
                _sessions = new Dictionary<string, SocketState>();
                _loggedOnCompIDs = new HashSet<string>();
            }

            public Dictionary<string, SocketState> GetSessions()
            {
                return _sessions;
            }
            
            public HashSet<string> GetLoggedOnCompIDs()
            {
                return _loggedOnCompIDs;
            }

            public void FromAdmin(Message message, SessionID sessionID)
            { }

            public void FromApp(Message message, SessionID sessionID)
            { }

            public void OnCreate(SessionID sessionID) { }
            public void OnLogout(SessionID sessionID)
            {
                _logoffNotify(sessionID.TargetCompID, _loggedOnCompIDs);
            }
            public void OnLogon(SessionID sessionID)
            {
                _logonNotify(sessionID.TargetCompID, _loggedOnCompIDs);
            }

            public void ToAdmin(Message message, SessionID sessionID) { }
            public void ToApp(Message message, SessionID sessionID) { }

        }

        private void LogonCallback(string compID, HashSet<string> loggedOnCompIDs)
        {
            lock (loggedOnCompIDs)
            {
                loggedOnCompIDs.Add(compID);
                Monitor.Pulse(loggedOnCompIDs);
            }
        }
        private void LogoffCallback(string compID, HashSet<string> loggedOnCompIDs)
        {
            lock (loggedOnCompIDs)
            {
                loggedOnCompIDs.Remove(compID);
                Monitor.Pulse(loggedOnCompIDs);
            }
        }

        public class SocketState
        {
            public SocketState(Socket s, Dictionary<string, SocketState> sessions)
            {
                _socket = s;
                _sessions = sessions;
            }
            public Socket _socket;
            public byte[] _rxBuffer = new byte[1024];
            public string _messageFragment = string.Empty;
            public string _exMessage;
            public Dictionary<string, SocketState> _sessions;
        }


        const int ConnectPort = 55100;
        private Dictionary CreateSessionConfig()
        {
            Dictionary settings = new Dictionary();
            settings.SetString(SessionSettings.CONNECTION_TYPE, "acceptor");
            settings.SetString(SessionSettings.USE_DATA_DICTIONARY, "N");
            settings.SetString(SessionSettings.START_TIME, "12:00:00");
            settings.SetString(SessionSettings.END_TIME, "12:00:00");
            settings.SetString(SessionSettings.HEARTBTINT, "300");
            settings.SetString(SessionSettings.SOCKET_CONNECT_HOST, Host);
            settings.SetString(SessionSettings.SOCKET_CONNECT_PORT, ConnectPort.ToString());
            settings.SetString(SessionSettings.SOCKET_ACCEPT_HOST, Host);
            settings.SetString(SessionSettings.SOCKET_ACCEPT_PORT, AcceptPort.ToString());
            settings.SetString(SessionSettings.DEBUG_FILE_LOG_PATH, Path.Combine(Path.GetDirectoryName(new Uri(Assembly.GetExecutingAssembly().CodeBase).LocalPath), "log"));
            return settings;
        }

        private SessionID CreateSessionID(string targetCompID)
        {
            return new SessionID(QuickFix.Values.BeginString_FIX42, ServerCompID, targetCompID);
        }

        private (ThreadedSocketAcceptor Acceptor, TestApplication App) CreateAcceptorFromSessionConfig()
        {
            TestApplication application = new TestApplication(LogonCallback, LogoffCallback);
            IMessageStoreFactory storeFactory = new MemoryStoreFactory();
            ILogFactory logFactory = new ScreenLogFactory(false, false, false);
            SessionSettings settings = new SessionSettings();

            settings.Set(CreateSessionID(StaticAcceptorCompID), CreateSessionConfig());
            settings.Set(CreateSessionID(StaticAcceptorCompID2), CreateSessionConfig());
            var acceptor = new ThreadedSocketAcceptor(application, storeFactory, settings, logFactory);
            return (acceptor, application);
        }

        private Socket ConnectToEngine(Dictionary<string, SocketState> sessions)
        {
            return ConnectToEngine( false, sessions );
        }

        private Socket ConnectToEngine( bool expectFailure, Dictionary<string, SocketState> sessions )
        {
            var address = IPAddress.Parse(Host);
            var endpoint = new IPEndPoint(address, AcceptPort);
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                socket.Connect(endpoint);
                ReceiveAsync(new SocketState(socket, sessions));
                return socket;
            }
            catch (Exception ex)
            {
                if (expectFailure) throw;
                Assert.Fail(string.Format("Failed to connect: {0}", ex.Message));
                return null;
            }
        }

        

        private void ReceiveAsync(SocketState socketState)
        {
            try
            {
                socketState._socket.BeginReceive(socketState._rxBuffer, 0, socketState._rxBuffer.Length, SocketFlags.None, new AsyncCallback(ProcessRXData), socketState);
            }
            catch (SocketException e)
            {
                if( e.SocketErrorCode != SocketError.Shutdown )
                {
                    throw;
                }
            }
        }

        private void ProcessRXData(IAsyncResult ar)
        {
            SocketState socketState = (SocketState)ar.AsyncState;
            int bytesReceived = 0;
            try
            {
                bytesReceived = socketState._socket.EndReceive(ar);
            }
            catch (Exception ex)
            {
                socketState._exMessage = ex.InnerException == null ? ex.Message : ex.InnerException.Message;
            }

            if (bytesReceived == 0)
            {
                socketState._socket.Close();
                lock (socketState._socket)
                    Monitor.Pulse(socketState._socket);
                return;
            }
            string msgText = Encoding.ASCII.GetString(socketState._rxBuffer, 0, bytesReceived);
            foreach (Match m in Regex.Matches(msgText, FIXMessageDelimit))
            {
                socketState._messageFragment += m.Value;
                if (Regex.IsMatch(socketState._messageFragment, FIXMessageEnd))
                {
                    Message message = new Message(socketState._messageFragment);
                    socketState._messageFragment = string.Empty;
                    string targetCompID = message.Header.GetField(QuickFix.Fields.Tags.TargetCompID);
                    if (message.Header.GetField(QuickFix.Fields.Tags.MsgType) == QuickFix.Fields.MsgType.LOGON)
                        lock (socketState._sessions)
                        {
                            socketState._sessions[targetCompID] = socketState;
                            Monitor.Pulse(socketState._sessions);
                        }
                    if (message.Header.GetField(QuickFix.Fields.Tags.MsgType) == QuickFix.Fields.MsgType.LOGOUT)
                        lock (socketState._sessions)
                        {
                            SendLogout(socketState._socket, targetCompID );
                            socketState._sessions.Remove( targetCompID );
                            Monitor.Pulse(socketState._sessions);
                        }
                }
            }
            ReceiveAsync(socketState);
        }



        private void SendLogon(Socket s, string senderCompID)
        {
            var msg = new QuickFix.FIX42.Logon();
            msg.Header.SetField(new QuickFix.Fields.TargetCompID(ServerCompID));
            msg.Header.SetField(new QuickFix.Fields.SenderCompID(senderCompID));
            msg.Header.SetField(new QuickFix.Fields.MsgSeqNum(_senderSequenceNumber++));
            msg.Header.SetField(new QuickFix.Fields.SendingTime(System.DateTime.UtcNow));
            msg.SetField(new QuickFix.Fields.HeartBtInt(300));
            s.Send(Encoding.ASCII.GetBytes(msg.ToString()));
        }

        private void SendLogout(Socket s, string senderCompID)
        {
            var msg = new QuickFix.FIX42.Logout();
            msg.Header.SetField(new QuickFix.Fields.TargetCompID(ServerCompID));
            msg.Header.SetField(new QuickFix.Fields.SenderCompID(senderCompID));
            msg.Header.SetField(new QuickFix.Fields.MsgSeqNum(_senderSequenceNumber++));
            msg.Header.SetField(new QuickFix.Fields.SendingTime(System.DateTime.UtcNow));
            s.Send(Encoding.ASCII.GetBytes(msg.ToString()));
        }

        private bool WaitForSessionStatus(string acceptorCompId, Dictionary<string, SocketState> sessions)
        {
            lock (sessions)
            {
                if (!sessions.ContainsKey(acceptorCompId))
                    Monitor.Wait(sessions, 10000);
                return sessions.ContainsKey(acceptorCompId);
            }
        }

        private bool WaitForLogonStatus(string targetCompID, HashSet<string> loggedOnCompIDs, int waitTime = 10000 )
        {
            lock (loggedOnCompIDs)
            {
                if (!loggedOnCompIDs.Contains(targetCompID))
                    Monitor.Wait(loggedOnCompIDs, waitTime);
                return loggedOnCompIDs.Contains(targetCompID);
            }
        }

        private bool WaitForDisconnect(Socket s)
        {
            lock (s)
            {
                if (s.Connected)
                    Monitor.Wait(s, 10000);
                return !s.Connected;
            }
        }

        [SetUp]
        public void Setup()
        {
            // _sessions = new Dictionary<string, SocketState>();
            // _loggedOnCompIDs = new HashSet<string>();
            _senderSequenceNumber = 1;
        }

        [TearDown]
        public void TearDown()
        {
            // if( _acceptor != null )
            // {
            //     _acceptor.Stop( true );
            //     Thread.Sleep(1500);
            //     _acceptor.Dispose();
            //     Thread.Sleep(1500);
            // }
            // _acceptor = null;
            if (_socket01.Connected)
            {
                DisconnectSocket(_socket01);
            }

            if (_socket02.Connected)
            {
                DisconnectSocket(_socket02);
            }
        }


        private ThreadedSocketAcceptor m_acceptor;
        // [Test]
        // public void TestAcceptorInStoppedStateOnInitialisation()
        // {
        //     //GIVEN - an acceptor
        //     var (acceptor, app) = CreateAcceptorFromSessionConfig();
        //     //THEN - is should be stopped
        //     Assert.IsFalse(acceptor.IsStarted);
        //     Assert.IsFalse(acceptor.AreSocketsRunning);
        //     Assert.IsFalse( acceptor.IsLoggedOn );
        //     if( acceptor != null )
        //     {
        //         acceptor.Stop( true );
        //         Thread.Sleep(1500);
        //         acceptor.Dispose();
        //         Thread.Sleep(1500);
        //     }
        //     acceptor = null;
        // }
        
        // [Test]
        // public void TestAcceptorInStoppedStateOnInitialisationThenCannotReceiveConnections()
        // {
        //     
        //     //GIVEN - an acceptor
        //     var (acceptor, app) = CreateAcceptorFromSessionConfig();
        //     //WHEN - a connection is received
        //     try
        //     {
        //         _socket01 = ConnectToEngine(true, app.GetSessions());
        //         // DisconnectSocket(_socket01);
        //     }
        //     //THEN - Expect failure to connect
        //     catch (SocketException)
        //     {
        //         //SUCCESS
        //     }
        // }
        
        // [Test]
        // public void TestCanStartAcceptor()
        // {
        //     //GIVEN - an acceptor
        //     var (acceptor, app) = CreateAcceptorFromSessionConfig();
        //     //WHEN - it is started
        //     acceptor.Start();
        //     //THEN - is be running
        //     Assert.IsTrue(acceptor.IsStarted);
        //     Assert.IsTrue( acceptor.AreSocketsRunning );
        //     Assert.IsFalse(acceptor.IsLoggedOn);
        //     if( acceptor != null )
        //     {
        //         acceptor.Stop( true );
        //         Thread.Sleep(1500);
        //         acceptor.Dispose();
        //         Thread.Sleep(1500);
        //     }
        //     acceptor = null;
        // }

        [Test]
        public void TestStartedAcceptorAndReceiveConnection()
        {
            //GIVEN - a started acceptor
            var (acceptor, app) = CreateAcceptorFromSessionConfig();
            acceptor.Start();
            //WHEN - a connection is received
            _socket01 = ConnectToEngine(app.GetSessions());
            SendLogon( _socket01, StaticAcceptorCompID );
            //THEN - is be running
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            Assert.IsTrue(acceptor.IsLoggedOn);

            //CLEANUP - disconnect client connections
            // DisconnectSocket(_socket01);
            if( acceptor != null )
            {
                acceptor.Stop( true );
                Thread.Sleep(1500);
                acceptor.Dispose();
                Thread.Sleep(1500);
            }
            acceptor = null;
        }

        [Test]
        public void TestStartedAcceptorAndReceiveConnection_Duplicate()
        {
            //GIVEN - a started acceptor
            var (acceptor, app) = CreateAcceptorFromSessionConfig();
            acceptor.Start();
            //WHEN - a connection is received
            _socket01 = ConnectToEngine(app.GetSessions());
            SendLogon( _socket01, StaticAcceptorCompID );
            //THEN - is be running
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            Assert.IsTrue(acceptor.IsLoggedOn);

            //CLEANUP - disconnect client connections
            // DisconnectSocket(_socket01);
            if( acceptor != null )
            {
                acceptor.Stop( true );
                Thread.Sleep(1500);
                acceptor.Dispose();
                Thread.Sleep(1500);
            }
            acceptor = null;
        }

        [Test]
        public void TestStartedAcceptorAndReceiveMultipleConnections()
        {
            //GIVEN - a started acceptor
            var (acceptor, app) = CreateAcceptorFromSessionConfig();
            acceptor.Start();
            //WHEN - a connection is received
            _socket01 = ConnectToEngine(app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            _socket02 = ConnectToEngine(app.GetSessions());
            SendLogon(_socket02, StaticAcceptorCompID2);
            //THEN - is be running
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session:" + StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID2, app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session:" + StaticAcceptorCompID2);
            Assert.IsTrue(acceptor.IsLoggedOn);
        
        
            //CLEANUP - disconnect client connections
            // DisconnectSocket(_socket02);
            // DisconnectSocket(_socket01);
            if( acceptor != null )
            {
                acceptor.Stop( true );
                Thread.Sleep(1500);
                acceptor.Dispose();
                Thread.Sleep(1500);
            }
            acceptor = null;
        }

        private void DisconnectSocket(Socket socket)
        {
            socket.Shutdown(SocketShutdown.Both);
            socket.Dispose();
        }
        
        // [Test]
        // public void TestCanStopAcceptor()
        // {
        //     //GIVEN - started acceptor
        //     var (acceptor, app) = CreateAcceptorFromSessionConfig();
        //     acceptor.Start();
        //     //WHEN - it is stopped
        //     acceptor.Stop();
        //     //THEN - it should no longer be running
        //     Assert.IsFalse(acceptor.IsStarted);
        //     Assert.IsFalse(acceptor.AreSocketsRunning);
        //     Assert.IsFalse(acceptor.IsLoggedOn);
        //     if( acceptor != null )
        //     {
        //         acceptor.Stop( true );
        //         Thread.Sleep(1500);
        //         acceptor.Dispose();
        //         Thread.Sleep(1500);
        //     }
        //     acceptor = null;
        // }
        
        // [Test]
        // public void TestCanForceStopAcceptorAndLogOffCounterpartyIfLoggedOn()
        // {
        //     //GIVEN - started acceptor with a logged on session
        //     var (acceptor, app) = CreateAcceptorFromSessionConfig();
        //     acceptor.Start();
        //     _socket01 = ConnectToEngine(app.GetSessions());
        //     SendLogon(_socket01, StaticAcceptorCompID);
        //     //WHEN - it is stopped with forced disconnection
        //     acceptor.Stop(true);
        //     //THEN - it should no longer be running
        //     Assert.IsTrue(WaitForDisconnect(_socket01), "Failed to disconnect session");
        //     Assert.IsFalse( acceptor.AreSocketsRunning );
        //     
        //     // DisconnectSocket(_socket01);
        //     if( acceptor != null )
        //     {
        //         acceptor.Stop( true );
        //         Thread.Sleep(1500);
        //         acceptor.Dispose();
        //         Thread.Sleep(1500);
        //     }
        //     acceptor = null;
        // }
        
        [Test]
        public void TestCanStopAcceptorAndLogOffCounterpartyIfLoggedOn()
        {
            //GIVEN - started acceptor with a logged on session
            var (acceptor, app) = CreateAcceptorFromSessionConfig();
            acceptor.Start();
            _socket01 = ConnectToEngine(app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            //WHEN - it is stopped
            acceptor.Stop();
            //THEN - it should no longer be running
            Assert.IsTrue(WaitForDisconnect(_socket01), "Failed to disconnect session");
            Assert.IsFalse( app.GetLoggedOnCompIDs().Contains( StaticAcceptorCompID ) );
            Assert.IsFalse(app.GetSessions().ContainsKey(StaticAcceptorCompID), "Failed to receive a logout message");
            Assert.IsFalse(acceptor.AreSocketsRunning);
            Assert.IsFalse( acceptor.IsLoggedOn );
            
            // DisconnectSocket(_socket01);
            if( acceptor != null )
            {
                acceptor.Stop( true );
                Thread.Sleep(1500);
                acceptor.Dispose();
                Thread.Sleep(1500);
            }
            acceptor = null;
        }

        // [Test]
        // public void TestCanRestartAcceptorAfterStopping()
        // {
        //     //GIVEN - a started then stopped acceptor 
        //     var (acceptor, app) = CreateAcceptorFromSessionConfig();
        //     acceptor.Start();
        //     acceptor.Stop();
        //     //WHEN - it is started again
        //     acceptor.Start();
        //     //THEN - it should be marked as running
        //     Assert.IsTrue(acceptor.IsStarted);
        //     Assert.IsTrue(acceptor.AreSocketsRunning);
        //     Assert.IsFalse(acceptor.IsLoggedOn);
        //     if( acceptor != null )
        //     {
        //         acceptor.Stop( true );
        //         Thread.Sleep(1500);
        //         acceptor.Dispose();
        //         Thread.Sleep(1500);
        //     }
        //     acceptor = null;
        // }
        
        [Test]
        public void TestCanRestartAcceptorAfterStoppingAndCounterPartyCanThenLogon()
        {
            //GIVEN - a started then stopped acceptor 
            var (acceptor, app) = CreateAcceptorFromSessionConfig();
            acceptor.Start();
            acceptor.Stop();
            //WHEN - it is started again
            acceptor.Start();
            //THEN - a counterparty should be able to logon
            Assert.IsTrue(acceptor.IsStarted);
            Assert.IsTrue(acceptor.AreSocketsRunning);
            _socket01 = ConnectToEngine(app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            Assert.IsTrue(acceptor.IsLoggedOn);
            Assert.IsTrue(WaitForSessionStatus(StaticAcceptorCompID, app.GetSessions()), "Logon messages was not recevied");
            
            // DisconnectSocket(_socket01);
            if( acceptor != null )
            {
                acceptor.Stop( true );
                Thread.Sleep(1500);
                acceptor.Dispose();
                Thread.Sleep(1500);
            }
            acceptor = null;
        }
        
        [Test]
        public void TestCanRestartAcceptorAndCounterPartyCanLogonAfterCounterParttyLoggedOnThenAcceptorStopped()
        {
            //GIVEN - a started then stopped acceptor 
            var (acceptor, app) = CreateAcceptorFromSessionConfig();
            acceptor.Start();
            _socket01 = ConnectToEngine(app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            acceptor.Stop();
            //WHEN - it is started again
            acceptor.Start();
            //THEN - a counterparty should be able to logon
            Assert.IsTrue(acceptor.IsStarted);
            Assert.IsTrue(acceptor.AreSocketsRunning);
            _socket01 = ConnectToEngine(app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            Assert.IsTrue(acceptor.IsLoggedOn);
            Assert.IsTrue( WaitForSessionStatus(StaticAcceptorCompID, app.GetSessions()));
            
            // DisconnectSocket(_socket01);
            if( acceptor != null )
            {
                acceptor.Stop( true );
                Thread.Sleep(1500);
                acceptor.Dispose();
                Thread.Sleep(1500);
            }
            acceptor = null;
        }
        
        
        [Test]
        public void TestCanRestartAcceptorAndCounterPartyCanLogonAfterCounterParttyLoggedOnThenAcceptorForceStopped()
        {
            //GIVEN - a started then stopped acceptor 
            var (acceptor, app) = CreateAcceptorFromSessionConfig();
            acceptor.Start();
            _socket01 = ConnectToEngine(app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            acceptor.Stop(true);
            //WHEN - it is started again
            acceptor.Start();
            //THEN - a counterparty should be able to logon
            Assert.IsTrue(acceptor.IsStarted);
            Assert.IsTrue(acceptor.AreSocketsRunning);
            _socket01 = ConnectToEngine(app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, app.GetLoggedOnCompIDs(),60000), "Failed to logon static acceptor session");
            Assert.IsTrue(acceptor.IsLoggedOn);
        
            //CLEANUP - disconnect client connections
            // DisconnectSocket(_socket01);
            if( acceptor != null )
            {
                acceptor.Stop( true );
                Thread.Sleep(1500);
                acceptor.Dispose();
                Thread.Sleep(1500);
            }
            acceptor = null;
        }
    }
}
