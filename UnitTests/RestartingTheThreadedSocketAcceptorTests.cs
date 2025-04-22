using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.CompilerServices;
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
        private ThreadedSocketAcceptor _acceptor = null;
        private TestApplication _app = null;
        private const string FIXMessageEnd = @"\x0110=\d{3}\x01";
        private const string FIXMessageDelimit = @"(8=FIX|\A).*?(" + FIXMessageEnd + @"|\z)";
        // private Dictionary<string, SocketState> _sessions;
        // private HashSet<string> _loggedOnCompIDs;
        private Socket _socket01 = null;
        private Socket _socket02 = null;
        private int _senderSequenceNumber = 1;
        private int _maxRetries = 5;

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
            _acceptor = new ThreadedSocketAcceptor(application, storeFactory, settings, logFactory);
            return (_acceptor, application);
        }

        private Socket ConnectToEngine(Dictionary<string, SocketState> sessions)
        {
            return ConnectToEngine( false, sessions );
        }

        private Socket ConnectToEngine( bool expectFailure, Dictionary<string, SocketState> sessions )
        {
            var address = IPAddress.Parse(Host);
            var endpoint = new IPEndPoint(address, AcceptPort);
            var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { Blocking = true };
            // for (int attempt = 1; attempt <= _maxRetries; attempt++)
            // {
                try
                {
                    socket.Connect(endpoint);
                    // IAsyncResult result = socket.BeginConnect(address, AcceptPort, null, null);
                    // bool success = result.AsyncWaitHandle.WaitOne(2000, true);
                    // if (!success)
                    // {
                    //     socket.Close();
                    //     Assert.Fail("Failed to connect");
                    // }
                    // socket.EndConnect(result);
                    ReceiveAsync(new SocketState(socket, sessions));
                    return socket;
                }
                catch (Exception ex)
                {
                    // if (attempt == _maxRetries)
                    // {
                        if (expectFailure) throw;
                        Assert.Fail(string.Format("Maxed out retries. Failed to connect: {0}", ex.Message));
                        return null;
                    // }
                    // Thread.Sleep(100);
                }
            // }

            // return socket;
        }

        private void RestartAcceptor()
        {
            _acceptor.Stop(true);
            _acceptor.Dispose();
            Thread.Sleep(1000); // Give OS time to release socket

            (_acceptor, _app) = CreateAcceptorFromSessionConfig(); // This recreates TcpListener
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

        private void AcceptorStart()
        {
            for (int i = 0; i <= _maxRetries; i++)
            {
                try
                {
                    _acceptor.Start();
                    break;
                }
                catch (Exception e)
                {
                    Thread.Sleep(100);
                    if (i == _maxRetries)
                    {
                        throw e;
                        // Assert.Fail(string.Format("Max retries: {0}", e.Message));
                    }
                }
            }
        }
        
        private void WaitForPortRelease(int retries = 10, int delayMs = 200)
        {
            for (int i = 0; i < retries; i++)
            {
                var props = IPGlobalProperties.GetIPGlobalProperties();
                var listeners = props.GetActiveTcpListeners();

                if (!listeners.Any(p => p.Port == AcceptPort))
                    return;

                Thread.Sleep(delayMs);
            }

            Assert.Fail($"Port {AcceptPort} was not released in time");
        }

        [SetUp]
        public void Setup()
        {
            // _sessions = new Dictionary<string, SocketState>();
            // _loggedOnCompIDs = new HashSet<string>();

            (_acceptor, _app) = CreateAcceptorFromSessionConfig();
            _senderSequenceNumber = 1;
            Console.WriteLine($"{_acceptor.GetType().FullName}@{RuntimeHelpers.GetHashCode(_acceptor):X}");
            if (_acceptor != null)
            {
                Console.WriteLine(_acceptor.AreSocketsRunning);
                _acceptor.Stop(true);
            }
        }

        [TearDown]
        public void TearDown()
        {
            try
            {
                // Stop sending/receiving
                _acceptor.Stop(true);// .Shutdown(SocketShutdown.Both);
                _acceptor.Dispose();
            }
            catch (SocketException)
            {
                // It might already be disconnected — ignore
            }
            catch (ObjectDisposedException)
            {
                // Already gone — ignore
            }
            if (_socket01 != null)
            {
                DisconnectSocket(_socket01);
                _socket01 = null;
            }
            if (_socket02 != null)
            {
                DisconnectSocket(_socket02);
                _socket02 = null;
            }
            Thread.Sleep(5000);
        }


        private ThreadedSocketAcceptor m_acceptor;
        [Test]
        public void TestAcceptorInStoppedStateOnInitialisation()
        {
            //GIVEN - an acceptor
            
            //THEN - is should be stopped
            Assert.IsFalse(_acceptor.IsStarted);
            Assert.IsFalse(_acceptor.AreSocketsRunning);
            Assert.IsFalse(_acceptor.IsLoggedOn);
        }
        
        [Test]
        public void TestAcceptorInStoppedStateOnInitialisationThenCannotReceiveConnections()
        {
            
            //GIVEN - an acceptor
            //WHEN - a connection is received
            try
            {
                _socket01 = ConnectToEngine(true, _app.GetSessions());
                // DisconnectSocket(_socket01);
            }
            //THEN - Expect failure to connect
            catch (SocketException)
            {
                //SUCCESS
            }
        }
        
        // [Test]
        // public void TestCanStartAcceptor()
        // {
        //     //GIVEN - an acceptor
        //     //WHEN - it is started
        //     // _acceptor.Start();
        //     AcceptorStart();
        //     //THEN - is be running
        //     Assert.IsTrue(_acceptor.IsStarted);
        //     Assert.IsTrue( _acceptor.AreSocketsRunning );
        //     Assert.IsFalse(_acceptor.IsLoggedOn);
        //     // if( acceptor != null )
        //     // {
        //     //     _acceptor.Stop( true );
        //     //     Thread.Sleep(1500);
        //     //     _acceptor.Dispose();
        //     //     Thread.Sleep(1500);
        //     // }
        //     // acceptor = null;
        // }

        [Test]
        public void TestStartedAcceptorAndReceiveConnection()
        {
            //GIVEN - a started acceptor
            // _acceptor.Start();
            AcceptorStart();
            //WHEN - a connection is received
            _socket01 = ConnectToEngine(_app.GetSessions());
            SendLogon( _socket01, StaticAcceptorCompID );
            //THEN - is be running
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, _app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            Assert.IsTrue(_acceptor.IsLoggedOn);
        
            //CLEANUP - disconnect client connections
            // DisconnectSocket(_socket01);
        }

        // [Test]
        // public void TestStartedAcceptorAndReceiveConnection_Duplicate()
        // {
        //     //GIVEN - a started acceptor
        //     // _acceptor.Start();
        //     AcceptorStart();
        //     //WHEN - a connection is received
        //     _socket01 = ConnectToEngine(_app.GetSessions());
        //     SendLogon( _socket01, StaticAcceptorCompID );
        //     //THEN - is be running
        //     Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, _app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
        //     Assert.IsTrue(_acceptor.IsLoggedOn);
        //
        //     //CLEANUP - disconnect client connections
        //     // DisconnectSocket(_socket01);
        //     // if( acceptor != null )
        //     // {
        //     //     _acceptor.Stop( true );
        //     //     Thread.Sleep(1500);
        //     //     _acceptor.Dispose();
        //     //     Thread.Sleep(1500);
        //     // }
        //     // acceptor = null;
        // }

        [Test]
        public void TestStartedAcceptorAndReceiveMultipleConnections()
        {
            //GIVEN - a started acceptor
            // _acceptor.Start();
            AcceptorStart();
            //WHEN - a connection is received
            _socket01 = ConnectToEngine(_app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            _socket02 = ConnectToEngine(_app.GetSessions());
            SendLogon(_socket02, StaticAcceptorCompID2);
            //THEN - is be running
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, _app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session:" + StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID2, _app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session:" + StaticAcceptorCompID2);
            Assert.IsTrue(_acceptor.IsLoggedOn);
        
        
            //CLEANUP - disconnect client connections
            // DisconnectSocket(_socket02);
            // DisconnectSocket(_socket01);
        }

        private void DisconnectSocket(Socket socket)
        {
            socket.Dispose();
        }
        
        [Test]
        public void TestCanStopAcceptor()
        {
            //GIVEN - started acceptor
            // _acceptor.Start();
            AcceptorStart();
            //WHEN - it is stopped
            _acceptor.Stop();
            //THEN - it should no longer be running
            Assert.IsFalse(_acceptor.IsStarted);
            Assert.IsFalse(_acceptor.AreSocketsRunning);
            Assert.IsFalse(_acceptor.IsLoggedOn);
        }
        
        [Test]
        public void TestCanForceStopAcceptorAndLogOffCounterpartyIfLoggedOn()
        {
            //GIVEN - started acceptor with a logged on session
            // _acceptor.Start();
            AcceptorStart();
            _socket01 = ConnectToEngine(_app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            //WHEN - it is stopped with forced disconnection
            _acceptor.Stop(true);
            //THEN - it should no longer be running
            Assert.IsTrue(WaitForDisconnect(_socket01), "Failed to disconnect session");
            Assert.IsFalse( _acceptor.AreSocketsRunning );
            
            // DisconnectSocket(_socket01);
        }
        
        [Test]
        public void TestCanStopAcceptorAndLogOffCounterpartyIfLoggedOn()
        {
            //GIVEN - started acceptor with a logged on session
            // _acceptor.Start();
            AcceptorStart();
            _socket01 = ConnectToEngine(_app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, _app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            //WHEN - it is stopped
            _acceptor.Stop();
            //THEN - it should no longer be running
            Assert.IsTrue(WaitForDisconnect(_socket01), "Failed to disconnect session");
            Assert.IsFalse( _app.GetLoggedOnCompIDs().Contains( StaticAcceptorCompID ) );
            Assert.IsFalse(_app.GetSessions().ContainsKey(StaticAcceptorCompID), "Failed to receive a logout message");
            Assert.IsFalse(_acceptor.AreSocketsRunning);
            Assert.IsFalse( _acceptor.IsLoggedOn );
            
            // DisconnectSocket(_socket01);
        }
        
        [Test]
        public void TestCanRestartAcceptorAfterStopping()
        {
            Thread.Sleep(10000);
            //GIVEN - a started then stopped acceptor 
            Console.WriteLine($"{_acceptor.GetType().FullName}@{RuntimeHelpers.GetHashCode(_acceptor):X}");
        
            Console.WriteLine(_acceptor.AreSocketsRunning);
            Console.WriteLine("STARTING");
            // _acceptor.Start();
            AcceptorStart();
            Console.WriteLine("STOPPING");
            // _acceptor.Stop(true);
            RestartAcceptor();
        
            // bool isAvailable = false;
            // IPGlobalProperties ipGlobalProperties = IPGlobalProperties.GetIPGlobalProperties();
            // TcpConnectionInformation[] tcpConnInfoArray = ipGlobalProperties.GetActiveTcpConnections();
            // int counter = 0;
            // while (!isAvailable)
            // {
            //     Console.WriteLine("Waiting for a connection...");
            //     Thread.Sleep(1000);
            //     foreach (TcpConnectionInformation tcpi in tcpConnInfoArray)
            //     {
            //         isAvailable = true;
            //         if (tcpi.LocalEndPoint.Port==AcceptPort)
            //         {
            //             isAvailable = false;
            //             break;
            //         }
            //     }
            //     counter++;
            //     if (counter == _maxRetries)
            //         Assert.Fail("Too many retries for TcpConnectionInformation");
            // }
            Thread.Sleep(200);
            //WHEN - it is started again
            Console.WriteLine("STARTING");
        
            // int maxRetries = 5;
            // for (int i = 0; i < maxRetries; i++)
            // {
            //     try
            //     {
            //         _acceptor.Start();
            //         break;
            //     }
            //     catch (Exception e)
            //     {
            //         Thread.Sleep(1000);
            //     }
            //
            //     if (i == maxRetries)
            //     {
            //         throw new Exception("Max retries reached");
            //     }
            // }
            // _acceptor.Start();
            AcceptorStart();
            
            //THEN - it should be marked as running
            Assert.IsTrue(_acceptor.IsStarted);
            Assert.IsTrue(_acceptor.AreSocketsRunning);
            Assert.IsFalse(_acceptor.IsLoggedOn);
        }
        
        [Test]
        public void TestCanRestartAcceptorAfterStoppingAndCounterPartyCanThenLogon()
        {
            //GIVEN - a started then stopped acceptor 
            // _acceptor.Start();
            AcceptorStart();
            // _acceptor.Stop();
            RestartAcceptor();
            //WHEN - it is started again
            // _acceptor.Start();
            AcceptorStart();
            //THEN - a counterparty should be able to logon
            Assert.IsTrue(_acceptor.IsStarted);
            Assert.IsTrue(_acceptor.AreSocketsRunning);
            _socket01 = ConnectToEngine(_app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, _app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            Assert.IsTrue(_acceptor.IsLoggedOn);
            Assert.IsTrue(WaitForSessionStatus(StaticAcceptorCompID, _app.GetSessions()), "Logon messages was not recevied");
            
            // DisconnectSocket(_socket01);
        }
        
        [Test]
        public void TestCanRestartAcceptorAndCounterPartyCanLogonAfterCounterParttyLoggedOnThenAcceptorStopped()
        {
            //GIVEN - a started then stopped acceptor
            // _acceptor.Start();
            AcceptorStart();
            _socket01 = ConnectToEngine(_app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, _app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            // _acceptor.Stop();
            RestartAcceptor();
            //WHEN - it is started again
            // _acceptor.Start();
            AcceptorStart();
            //THEN - a counterparty should be able to logon
            Assert.IsTrue(_acceptor.IsStarted);
            Assert.IsTrue(_acceptor.AreSocketsRunning);
            _socket01 = ConnectToEngine(_app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, _app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            Assert.IsTrue(_acceptor.IsLoggedOn);
            Assert.IsTrue( WaitForSessionStatus(StaticAcceptorCompID, _app.GetSessions()));
            
            // DisconnectSocket(_socket01);
        }
        
        
        [Test]
        public void TestCanRestartAcceptorAndCounterPartyCanLogonAfterCounterParttyLoggedOnThenAcceptorForceStopped()
        {
            //GIVEN - a started then stopped acceptor 
            // _acceptor.Start();
            AcceptorStart();
            _socket01 = ConnectToEngine(_app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, _app.GetLoggedOnCompIDs()), "Failed to logon static acceptor session");
            
            // _acceptor.Stop(true);
            // WaitForPortRelease();
            RestartAcceptor();
            
            //Experimental
            // Thread.Sleep(2000);
            // DisconnectSocket(_socket01);
            // _acceptor.Stop(true);
            // _acceptor.Dispose();
            // (_acceptor, _app) = CreateAcceptorFromSessionConfig();
            // bool isListening = false;
            // var address = IPAddress.Parse(Host);
            // for (int attempt = 0; attempt <= _maxRetries; attempt++)
            // {
            //     var props = IPGlobalProperties.GetIPGlobalProperties();
            //     isListening = props.GetActiveTcpListeners()
            //         .Any(p => p.Port == AcceptPort && p.Address.Equals(address));
            //     if (isListening)
            //         break;
            //     else
            //     {
            //         if (attempt == _maxRetries)
            //             Assert.Fail("Maxed out listening for AcceptPort");
            //     }
            //     Thread.Sleep(1000);
            // }
            // Thread.Sleep(2000);
            
            //WHEN - it is started again
            // _acceptor.Start();
            Console.WriteLine("Port 55101 bound? " + IPGlobalProperties.GetIPGlobalProperties()
              .GetActiveTcpListeners()
              .Any(p => p.Port == AcceptPort));

            AcceptorStart();
            
            // Console.WriteLine(_acceptor.GetAcceptorAddresses());
            //THEN - a counterparty should be able to logon
            Assert.IsTrue(_acceptor.IsStarted);
            Assert.IsTrue(_acceptor.AreSocketsRunning);
            _socket01 = ConnectToEngine(_app.GetSessions());
            SendLogon(_socket01, StaticAcceptorCompID);
            Assert.IsTrue(WaitForLogonStatus(StaticAcceptorCompID, _app.GetLoggedOnCompIDs(),60000), "Failed to logon static acceptor session");
            Assert.IsTrue(_acceptor.IsLoggedOn);
        
            //CLEANUP - disconnect client connections
            // DisconnectSocket(_socket01);
        }
    }
}
