#region Using directives
using System;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.HMIProject;
using FTOptix.Retentivity;
using FTOptix.NativeUI;
using FTOptix.NetLogic;
using FTOptix.UI;
using FTOptix.CoreBase;
using FTOptix.Core;
using RATC.ETHIP;
using static RATC.ETHIP.Encapsulation;
using System.Threading.Tasks;
using System.IO;
using System.Text;
using FTOptix.Store;
using FTOptix.ODBCStore;
using FTOptix.SQLiteStore;
using FTOptix.AuditSigning;
using FTOptix.DataLogger;
using FTOptix.System;
using System.Timers;
using FTOptix.OPCUAServer;
using FTOptix.EventLogger;
using FTOptix.RAEtherNetIP;
using FTOptix.CommunicationDriver;
using System.Collections.Generic;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using System.Net;
#endregion

public class raC_ETHIPDeviceLogic : BaseNetLogic
{
    /// <summary>
    /// The generic EthernetIP client
    /// </summary>
    private RATC.ETHIP.ETHIPClient _ethipClient;
    /// <summary>
    /// Reference to the Path Variable of the device
    /// </summary>
    private IUAVariable _PathVariable;
    private IUAVariable _RouteVariable;

    private System.Timers.Timer _tmrUpdateIdentity;

    private System.Timers.Timer _tmrReconnect;

    public override void Start()
    {
        //Check for variable existence
        _PathVariable = Owner.GetVariable("Path");
        if (_PathVariable == null)
            throw new CoreConfigurationException("Path variable not found");

        _RouteVariable = Owner.GetVariable("Route");
        if (_RouteVariable == null)
            throw new CoreConfigurationException("Route variable not found");

        _ethipClient = new ETHIPClient();

        //Subscribe to the path value change event
        _PathVariable.VariableChange += Path_VariableChange;
        _RouteVariable.VariableChange += Route_VariableChange;
        string dest = _PathVariable.Value;

        _ethipClient.IPAddress = _PathVariable.Value;
        try
        {
            _ethipClient.RoutePath = _RouteVariable.Value;
        }
        catch (Exception ex)
        {
            //Log.Error("Invalid route '" + _RouteVariable.Value + "' of device " + Owner.BrowseName);
        }

        //_identityTask = new PeriodicTask(getIdentifyItem,10000,Owner);
        //ToDo: Make this a configuration variable
        _tmrUpdateIdentity = new System.Timers.Timer(10000);
        _tmrUpdateIdentity.Elapsed += updateIdentity;
        _tmrReconnect = new System.Timers.Timer(3000);
        _tmrReconnect.Elapsed += reconnect;

        foreach (var item in Owner.Children)
        {
            if (item is raC_ETHIPTagGroupCore)
            {
                if (item.GetVariable("CyclicReadEnabled/AutoStart").Value)
                {
                    item.GetByType<NetLogicObject>().ExecuteMethod("StartGroup");
                }
            }
        }

        _tmrReconnect.Start();
    }

    private void Route_VariableChange(object sender, VariableChangeEventArgs e)
    {
        _ethipClient.RoutePath = _RouteVariable.Value;
        getIdentifyItem();
    }

    /// <summary>
    /// Path configuration changed
    /// </summary>
    /// <param name="sender"></param>
    /// <param name="e"></param>
    private void Path_VariableChange(object sender, VariableChangeEventArgs e)
    {
        //Unregister client if allready connected
        //TODO: look at ETHIP group for a better check
        _tmrReconnect.Stop();
        _tmrUpdateIdentity.Stop();
        if (_ethipClient != null)
            _ethipClient.UnRegisterSession();

        //Create new instance
        _ethipClient = new ETHIPClient();
        _ethipClient.IPAddress = _PathVariable.Value;
        _ethipClient.RoutePath = _RouteVariable.Value;

        _tmrReconnect.Start();
    }

    public override void Stop()
    {
        //Cleanup event subscription and unregister client
        _tmrReconnect.Elapsed -= reconnect;
        _tmrReconnect.Stop();
        _PathVariable.VariableChange -= Path_VariableChange;

        if (_tmrUpdateIdentity != null)
            _tmrUpdateIdentity.Stop();

        foreach (var item in Owner.Children)
        {
            if (item is raC_ETHIPTagGroupCore)
            {
                if (item.GetVariable("CyclicReadEnabled/AutoStart").Value)
                {
                    item.GetByType<NetLogicObject>().ExecuteMethod("StopGroup");
                }
            }
        }

        //Unregister CIP session 
        if (_ethipClient != null)
        {
            try
            {
                _ethipClient.UnRegisterSession();
                //_ethipClient.Client.Close();
            }
            catch
            {

            }
        }
    }

    private void reconnect(object source, ElapsedEventArgs e)
    {
        _tmrReconnect.Stop();
        getIdentifyItem();
    }

    private void updateIdentity(object source, ElapsedEventArgs e)
    {
        _tmrUpdateIdentity.Stop();
        getIdentifyItem();
    }

    /// <summary>
    /// Gets the CIP Identity Item
    /// </summary>
    [FTOptix.NetLogic.ExportMethod]
    public void getIdentifyItem()
    {
        raC_ETHIPDevice d = (raC_ETHIPDevice)Owner;

        try
        {
            //Register CIP session
            _tmrUpdateIdentity.Stop();
            _ethipClient.RegisterSession();

            //send get attribute all command to the device
            BinaryReader res = _ethipClient.GetAttributeAll(1, 1);

            //set values
            d.GetVariable("Indentity/VendorID").Value = res.ReadInt16();
            d.GetVariable("Indentity/DeviceType").Value = res.ReadInt16();
            d.GetVariable("Indentity/ProductCode").Value = res.ReadInt16();
            d.GetVariable("Indentity/Major").Value = res.ReadByte();
            d.GetVariable("Indentity/Minor").Value = res.ReadByte();
            d.GetVariable("Indentity/Status").Value = res.ReadInt16();
            d.GetVariable("Indentity/Serial").Value = res.ReadInt32();
            //This is a so called "short string" having just one byte as length
            int len = res.ReadByte();
            d.GetVariable("Indentity").Value = System.Text.Encoding.ASCII.GetString(res.ReadBytes(len));

            //byte[] res2 = _ethipClient.GetAttributeSingle(1,1,21);
            //len = res2[0];
            //d.GetVariable("Catalog").Value = System.Text.Encoding.ASCII.GetString(res2,1,len);
            if (res.PeekChar() >= 0)
            {
                len = res.ReadByte();
                d.GetVariable("Indentity/Catalog").Value = System.Text.Encoding.ASCII.GetString(res.ReadBytes(len));
            }
            else
                d.GetVariable("Indentity/Catalog").Value = "";

            if (d.GetVariable("cfgRefreshIdentity").Value)
                _tmrUpdateIdentity.Start();

            //identityStarted=true;
            d.GetVariable("ConnectionStatus").Value = true;
        }
        catch (System.Exception ex)
        {
            Log.Error(Owner.BrowseName + ": " + ex.Message + Environment.NewLine + ex.StackTrace.ToString());
            clearIdentity();
            d.GetVariable("ConnectionStatus").Value = false;
            if (d.GetVariable("cfgRefreshIdentity").Value)
                _tmrReconnect.Start();

            _ethipClient.UnRegisterSession();
        }
        finally
        {
            //Mirror the route info, so that one knows the processing was done after changing the route of the device.
            d.GetVariable("Indentity/Route").Value = _RouteVariable.Value;
        }
    }

    private void clearIdentity()
    {
        raC_ETHIPDevice d = (raC_ETHIPDevice)Owner;

        //set values
        d.GetVariable("Indentity/VendorID").Value = 0;
        d.GetVariable("Indentity/DeviceType").Value = 0;
        d.GetVariable("Indentity/ProductCode").Value = 0;
        d.GetVariable("Indentity/Major").Value = 0;
        d.GetVariable("Indentity/Minor").Value = 0;
        d.GetVariable("Indentity/Status").Value = 0;
        d.GetVariable("Indentity/Serial").Value = 0;
        d.GetVariable("Indentity").Value = "ERR";
        d.GetVariable("Indentity/Catalog").Value = "";
    }

    public void getSingleParameterCIA(int Class, int Instance, int Attribute, out byte[] res)
    {
        try
        {
            _ethipClient.RegisterSession();
            res = _ethipClient.GetAttributeSingle(Class, Instance, Attribute);
        }
        catch (Exception ex)
        {
            if (_ethipClient.SessionHandle != 0)
                Log.Error(Owner.BrowseName + ": " + ex.Message + Environment.NewLine + ex.StackTrace);
            res = new byte[0];
            throw;
        }
    }

    public void setSingleParameterCIA(int Class, int Instance, int Attribute, byte[] data)
    {
        try
        {
            _ethipClient.RegisterSession();
            _ethipClient.SetAttributeSingle(Class, Instance, Attribute, data);
        }
        catch (Exception ex)
        {
            if (_ethipClient.SessionHandle != 0)
                Log.Error(Owner.BrowseName + ": " + ex.Message + Environment.NewLine + ex.StackTrace);

            throw;
        }
    }

    /// <summary>
    /// Gets the CIP Identity Item
    /// </summary>
    //[FTOptix.NetLogic.ExportMethod]
    public void executeCustomService(int ServiceCode, int ClassID, int InstanceID, int AttributeID, byte[] dataToSend, out byte[] dataRecieved)
    {
        try
        {
            _ethipClient.RegisterSession();

            dataRecieved = _ethipClient.CustomService(ServiceCode, ClassID, InstanceID, AttributeID, dataToSend);
        }
        catch (Exception ex)
        {
            if (_ethipClient.SessionHandle != 0)
                Log.Error(Owner.BrowseName + ": " + ex.Message + Environment.NewLine + ex.StackTrace);
            dataRecieved = new byte[0];
            throw;
        }
    }

}


namespace RATC.ETHIP
{
    public class scatteredItem
    {
        public byte[] Attribute;
        public byte[] Value;
        public raC_ETHIPTagCore TagObj;
    }

    public class Encapsulation
    {
        public CommandsEnum Command { get; set; }
        public UInt16 Length { get; set; }
        public UInt32 SessionHandle { get; set; }
        public StatusEnum Status { get; }
        private byte[] senderContext = new byte[8];
        private UInt32 options = 0;
        public List<byte> CommandSpecificData = new List<byte>();

        /// <summary>
        /// Table 2-3.3 Error Codes
        /// </summary>
        public enum StatusEnum : UInt32
        {
            Success = 0x0000,
            InvalidCommand = 0x0001,
            InsufficientMemory = 0x0002,
            IncorrectData = 0x0003,
            InvalidSessionHandle = 0x0064,
            InvalidLength = 0x0065,
            UnsupportedEncapsulationProtocol = 0x0069
        }





        /// <summary>
        /// Table 2-3.2 Encapsulation Commands
        /// </summary>
        public enum CommandsEnum : UInt16
        {
            NOP = 0x0000,
            ListServices = 0x0004,
            ListIdentity = 0x0063,
            ListInterfaces = 0x0064,
            RegisterSession = 0x0065,
            UnRegisterSession = 0x0066,
            SendRRData = 0x006F,
            SendUnitData = 0x0070,
            IndicateStatus = 0x0072,
            Cancel = 0x0073
        }

        public byte[] toBytes()
        {
            byte[] returnValue = new byte[24 + CommandSpecificData.Count];
            returnValue[0] = (byte)this.Command;
            returnValue[1] = (byte)((UInt16)this.Command >> 8);
            returnValue[2] = (byte)this.Length;
            returnValue[3] = (byte)((UInt16)this.Length >> 8);
            returnValue[4] = (byte)this.SessionHandle;
            returnValue[5] = (byte)((UInt32)this.SessionHandle >> 8);
            returnValue[6] = (byte)((UInt32)this.SessionHandle >> 16);
            returnValue[7] = (byte)((UInt32)this.SessionHandle >> 24);
            returnValue[8] = (byte)this.Status;
            returnValue[9] = (byte)((UInt16)this.Status >> 8);
            returnValue[10] = (byte)((UInt16)this.Status >> 16);
            returnValue[11] = (byte)((UInt16)this.Status >> 24);
            returnValue[12] = senderContext[0];
            returnValue[13] = senderContext[1];
            returnValue[14] = senderContext[2];
            returnValue[15] = senderContext[3];
            returnValue[16] = senderContext[4];
            returnValue[17] = senderContext[5];
            returnValue[18] = senderContext[6];
            returnValue[19] = senderContext[7];
            returnValue[20] = (byte)this.options;
            returnValue[21] = (byte)((UInt16)this.options >> 8);
            returnValue[22] = (byte)((UInt16)this.options >> 16);
            returnValue[23] = (byte)((UInt16)this.options >> 24);
            for (int i = 0; i < CommandSpecificData.Count; i++)
            {
                returnValue[24 + i] = CommandSpecificData[i];
            }
            return returnValue;
        }


        /// <summary>
        /// Table 2-4.4 CIP Identity Item
        /// </summary>
        public class CIPIdentityItem
        {
            public UInt16 ItemTypeCode;                                     //Code indicating item type of CIP Identity (0x0C)
            public UInt16 ItemLength;                                       //Number of bytes in item which follow (length varies depending on Product Name string)
            public UInt16 EncapsulationProtocolVersion;                     //Encapsulation Protocol Version supported (also returned with Register Sesstion reply).
            public SocketAddress SocketAddress = new SocketAddress();       //Socket Address (see section 2-6.3.2)
            public UInt16 VendorID1;                                        //Device manufacturers Vendor ID
            public UInt16 DeviceType1;                                      //Device Type of product
            public UInt16 ProductCode1;                                     //Product Code assigned with respect to device type
            public byte[] Revision1 = new byte[2];                          //Device revision
            public UInt16 Status1;                                          //Current status of device
            public UInt32 SerialNumber1;                                      //Serial number of device
            public byte ProductNameLength;
            public string ProductName1;                                     //Human readable description of device
            public byte State1;                                             //Current state of device


            public static CIPIdentityItem getCIPIdentityItem(int startingByte, byte[] receivedData)
            {
                startingByte = startingByte + 2;            //Skipped ItemCount
                CIPIdentityItem cipIdentityItem = new CIPIdentityItem();
                cipIdentityItem.ItemTypeCode = Convert.ToUInt16(receivedData[0 + startingByte]
                                                                    | (receivedData[1 + startingByte] << 8));
                cipIdentityItem.ItemLength = Convert.ToUInt16(receivedData[2 + startingByte]
                                                                    | (receivedData[3 + startingByte] << 8));
                cipIdentityItem.EncapsulationProtocolVersion = Convert.ToUInt16(receivedData[4 + startingByte]
                                                                    | (receivedData[5 + startingByte] << 8));
                cipIdentityItem.SocketAddress.SIN_family = Convert.ToUInt16(receivedData[7 + startingByte]
                                                    | (receivedData[6 + startingByte] << 8));
                cipIdentityItem.SocketAddress.SIN_port = Convert.ToUInt16(receivedData[9 + startingByte]
                                                    | (receivedData[8 + startingByte] << 8));
                cipIdentityItem.SocketAddress.SIN_Address = (UInt32)(receivedData[13 + startingByte]
                                                    | (receivedData[12 + startingByte] << 8)
                                                    | (receivedData[11 + startingByte] << 16)
                                                    | (receivedData[10 + startingByte] << 24)
                                                    );
                cipIdentityItem.VendorID1 = Convert.ToUInt16(receivedData[22 + startingByte]
                                    | (receivedData[23 + startingByte] << 8));
                cipIdentityItem.DeviceType1 = Convert.ToUInt16(receivedData[24 + startingByte]
                                    | (receivedData[25 + startingByte] << 8));
                cipIdentityItem.ProductCode1 = Convert.ToUInt16(receivedData[26 + startingByte]
                    | (receivedData[27 + startingByte] << 8));
                cipIdentityItem.Revision1[0] = receivedData[28 + startingByte];
                cipIdentityItem.Revision1[1] = receivedData[29 + startingByte];
                cipIdentityItem.Status1 = Convert.ToUInt16(receivedData[30 + startingByte]
                    | (receivedData[31 + startingByte] << 8));
                cipIdentityItem.SerialNumber1 = (UInt32)(receivedData[32 + startingByte]
                                                    | (receivedData[33 + startingByte] << 8)
                                                    | (receivedData[34 + startingByte] << 16)
                                                    | (receivedData[35 + startingByte] << 24));
                cipIdentityItem.ProductNameLength = receivedData[36 + startingByte];
                cipIdentityItem.ProductName1 = System.Text.Encoding.ASCII.GetString(receivedData, 37 + startingByte, cipIdentityItem.ProductNameLength);
                cipIdentityItem.State1 = receivedData[receivedData.Length - 1];
                return cipIdentityItem;
            }
            /// <summary>
            /// Converts an IP-Address in UIint32 Format (Received by Device)
            /// </summary>
            public static string getIPAddress(UInt32 address)
            {
                return ((byte)(address >> 24)).ToString() + "." + ((byte)(address >> 16)).ToString() + "." + ((byte)(address >> 8)).ToString() + "." + ((byte)(address)).ToString();
            }


        }




        /// <summary>
        /// Socket Address (see section 2-6.3.2)
        /// </summary>
        public class SocketAddress
        {
            public UInt16 SIN_family;
            public UInt16 SIN_port;
            public UInt32 SIN_Address;
            public byte[] SIN_Zero = new byte[8];
        }

        public class CommonPacketFormat
        {
            public UInt16 ItemCount = 2;
            public UInt16 AddressItem = 0x0000;
            public UInt16 AddressLength = 0;
            public UInt16 DataItem = 0xB2; //0xB2 = Unconnected Data Item
            public UInt16 DataLength = 8;
            public List<byte> Data = new List<byte>();
            public UInt16 SockaddrInfoItem_O_T = 0x8001; //8000 for O->T and 8001 for T->O - Volume 2 Table 2-6.9
            public UInt16 SockaddrInfoLength = 16;
            public SocketAddress SocketaddrInfo_O_T = null;


            public byte[] toBytes()
            {
                if (SocketaddrInfo_O_T != null)
                    ItemCount = 3;
                byte[] returnValue = new byte[10 + Data.Count + (SocketaddrInfo_O_T == null ? 0 : 20)];
                returnValue[0] = (byte)this.ItemCount;
                returnValue[1] = (byte)((UInt16)this.ItemCount >> 8);
                returnValue[2] = (byte)this.AddressItem;
                returnValue[3] = (byte)((UInt16)this.AddressItem >> 8);
                returnValue[4] = (byte)this.AddressLength;
                returnValue[5] = (byte)((UInt16)this.AddressLength >> 8);
                returnValue[6] = (byte)this.DataItem;
                returnValue[7] = (byte)((UInt16)this.DataItem >> 8);
                returnValue[8] = (byte)this.DataLength;
                returnValue[9] = (byte)((UInt16)this.DataLength >> 8);
                for (int i = 0; i < Data.Count; i++)
                {
                    returnValue[10 + i] = Data[i];
                }


                // Add Socket Address Info Item
                if (SocketaddrInfo_O_T != null)
                {
                    returnValue[10 + Data.Count + 0] = (byte)this.SockaddrInfoItem_O_T;
                    returnValue[10 + Data.Count + 1] = (byte)((UInt16)this.SockaddrInfoItem_O_T >> 8);
                    returnValue[10 + Data.Count + 2] = (byte)this.SockaddrInfoLength;
                    returnValue[10 + Data.Count + 3] = (byte)((UInt16)this.SockaddrInfoLength >> 8);
                    returnValue[10 + Data.Count + 5] = (byte)this.SocketaddrInfo_O_T.SIN_family;
                    returnValue[10 + Data.Count + 4] = (byte)((UInt16)this.SocketaddrInfo_O_T.SIN_family >> 8);
                    returnValue[10 + Data.Count + 7] = (byte)this.SocketaddrInfo_O_T.SIN_port;
                    returnValue[10 + Data.Count + 6] = (byte)((UInt16)this.SocketaddrInfo_O_T.SIN_port >> 8);
                    returnValue[10 + Data.Count + 11] = (byte)this.SocketaddrInfo_O_T.SIN_Address;
                    returnValue[10 + Data.Count + 10] = (byte)((UInt32)this.SocketaddrInfo_O_T.SIN_Address >> 8);
                    returnValue[10 + Data.Count + 9] = (byte)((UInt32)this.SocketaddrInfo_O_T.SIN_Address >> 16);
                    returnValue[10 + Data.Count + 8] = (byte)((UInt32)this.SocketaddrInfo_O_T.SIN_Address >> 24);
                    returnValue[10 + Data.Count + 12] = this.SocketaddrInfo_O_T.SIN_Zero[0];
                    returnValue[10 + Data.Count + 13] = this.SocketaddrInfo_O_T.SIN_Zero[1];
                    returnValue[10 + Data.Count + 14] = this.SocketaddrInfo_O_T.SIN_Zero[2];
                    returnValue[10 + Data.Count + 15] = this.SocketaddrInfo_O_T.SIN_Zero[3];
                    returnValue[10 + Data.Count + 16] = this.SocketaddrInfo_O_T.SIN_Zero[4];
                    returnValue[10 + Data.Count + 17] = this.SocketaddrInfo_O_T.SIN_Zero[5];
                    returnValue[10 + Data.Count + 18] = this.SocketaddrInfo_O_T.SIN_Zero[6];
                    returnValue[10 + Data.Count + 19] = this.SocketaddrInfo_O_T.SIN_Zero[7];
                }
                return returnValue;
            }
        }
    }

    /// <summary>
    /// Table A-3.1 Volume 1 Chapter A-3
    /// </summary>
    public enum CIPServices : byte
    {
        Get_Attributes_All = 0x01,
        Set_Attributes_All_Request = 0x02,
        Get_Attribute_List = 0x03,
        Set_Attribute_List = 0x04,
        Reset = 0x05,
        Start = 0x06,
        Stop = 0x07,
        Create = 0x08,
        Delete = 0x09,
        Multiple_Service_Packet = 0x0A,
        Apply_Attributes = 0x0D,
        Get_Attribute_Single = 0x0E,
        Set_Attribute_Single = 0x10,
        Find_Next_Object_Instance = 0x11,
        Error_Response = 0x14,
        Restore = 0x15,
        Save = 0x16,
        NOP = 0x17,
        Get_Member = 0x18,
        Set_Member = 0x19,
        Insert_Member = 0x1A,
        Remove_Member = 0x1B,
        GroupSync = 0x1C,
        ScatteredRead = 0x4d,
        ScatteredWrite = 0x4e
    }


    public class CIPException : Exception
    {
        public CIPException()
        {
        }

        public CIPException(string message)
            : base(message)
        {
        }

        public CIPException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }

    /// <summary>
    /// Table B-1.1 CIP General Status Codes
    /// </summary>
    internal static class GeneralStatusCodes
    {
        static internal string GetStatusCode(byte code)
        {
            switch (code)
            {
                case 0x00: return "Success";
                case 0x01: return "Connection failure";
                case 0x02: return "Resource unavailable";
                case 0x03: return "Invalid Parameter value";
                case 0x04: return "Path segment error";
                case 0x05: return "Path destination unknown";
                case 0x06: return "Partial transfer";
                case 0x07: return "Connection lost";
                case 0x08: return "Service not supported";
                case 0x09: return "Invalid attribute value";
                case 0x0A: return "Attribute List error";
                case 0x0B: return "Already in requested mode/state";
                case 0x0C: return "Object state conflict";
                case 0x0D: return "Object already exists";
                case 0x0E: return "Attribute not settable";
                case 0x0F: return "Privilege violation";
                case 0x10: return "Device state conflict";
                case 0x11: return "Reply data too large";
                case 0x12: return "Fragmentation of a primitive value";
                case 0x13: return "Not enough data";
                case 0x14: return "Attribute not supported";
                case 0x15: return "Too much data";
                case 0x16: return "Object does not exist";
                case 0x17: return "Service fragmentation sequence not in progress";
                case 0x18: return "No stored attribute data";
                case 0x19: return "Store operation failure";
                case 0x1A: return "Routing failure, request packet too large";
                case 0x1B: return "Routing failure, response packet too large";
                case 0x1C: return "Missing attribute list entry data";
                case 0x1D: return "Invalid attribute value list";
                case 0x1E: return "Embedded service error";
                case 0x1F: return "Vendor specific error";
                case 0x20: return "Invalid parameter";
                case 0x21: return "Write-once value or medium atready written";
                case 0x22: return "Invalid Reply Received";
                case 0x23: return "Buffer overflow";
                case 0x24: return "Message format error";
                case 0x25: return "Key failure path";
                case 0x26: return "Path size invalid";
                case 0x27: return "Unecpected attribute list";
                case 0x28: return "Invalid Member ID";
                case 0x29: return "Member not settable";
                case 0x2A: return "Group 2 only Server failure";
                case 0x2B: return "Unknown Modbus Error";
                default: return "unknown";
            }
        }
        /// <summary>
        /// Returns the Explanation of a given statuscode (Table 3-5-29) Page 3-75 Vol 1
        /// </summary>
        /// <param name="statusCode">Extended Status Code</param> 
        public static string GetExtendedStatus(uint statusCode)
        {
            switch (statusCode)
            {
                case 0x0100: return "Connection in use or duplicate forward open";
                case 0x0103: return "Transport class and trigger combination not supported";
                case 0x0106: return "Ownership conflict";
                case 0x0107: return "Target connection not found";
                case 0x0108: return "Invalid network connection parameter";
                case 0x0109: return "Invalid connection size";
                case 0x0110: return "Target for connection not configured";
                case 0x0111: return "RPI not supported";
                case 0x0113: return "Out of connections";
                case 0x0114: return "Vendor ID or product code missmatch";
                case 0x0115: return "Product type missmatch";
                case 0x0116: return "Revision mismatch";
                case 0x0117: return "Invalid produced or consumed application path";
                case 0x0118: return "Invalid or inconsistent configuration application path";
                case 0x0119: return "non-listen only connection not opened";
                case 0x011A: return "Target Obbject out of connections";
                case 0x011B: return "RPI is smaller than the production inhibit time";
                case 0x0203: return "Connection timed out";
                case 0x0204: return "Unconnected request timed out";
                case 0x0205: return "Parameter Error in unconnected request service";
                case 0x0206: return "Message too large for unconnected_send service";
                case 0x0207: return "Unconnected acknowledge without reply";
                case 0x0301: return "No Buffer memory available";
                case 0x0302: return "Network Bandwidth not available for data";
                case 0x0303: return "No consumed connection ID Filter available";
                case 0x0304: return "Not configured to send Scheduled priority data";
                case 0x0305: return "Schedule signature missmatch";
                case 0x0306: return "Schedule signature validation not possible";
                case 0x0311: return "Port not available";
                case 0x0312: return "Link address not valid";
                case 0x0315: return "Invalid segment in connection path";
                case 0x0316: return "Error in forward close service connection path";
                case 0x0317: return "Scheduling not specified";
                case 0x0318: return "Link address to self invalid";
                case 0x0319: return "Secondary resources unavailable";
                case 0x031A: return "Rack connation already established";
                case 0x031B: return "Module connection already established";
                case 0x031C: return "Miscellaneous";
                case 0x031D: return "Redundant connection Mismatch";
                case 0x031E: return "No more user configurable link consumer resources available in the producing module";
                case 0x0800: return "Network link in path module is offline";
                case 0x0810: return "No target application data available";
                case 0x0811: return "No originator application data available";
                case 0x0812: return "Node address has chnged since the network was scheduled";
                case 0x0813: return "Not configured for off-Subnet Multicast";
                default: return "unknown";
            }
        }
    }
    public class ETHIPClient
    {
        private TcpClient client;
        NetworkStream stream;
        public UInt32 SessionHandle
        {
            get
            { return _sessionHandle; }
        }
        UInt32 _sessionHandle;
        UInt32 connectionID_O_T;
        UInt32 connectionID_T_O;
        UInt32 multicastAddress;
        UInt16 connectionSerialNumber;
        /// <summary>
        /// TCP-Port of the Server
        /// </summary>
        public ushort TCPPort { get; set; } = 0xAF12;
        /// <summary>
        /// UDP-Port of the IO-Adapter - Standard is 0xAF12
        /// </summary>
        public ushort TargetUDPPort { get; set; } = 0x08AE;
        /// <summary>
        /// UDP-Port of the Scanner - Standard is 0xAF12
        /// </summary>
        public ushort OriginatorUDPPort { get; set; } = 0x08AE;
        /// <summary>
        /// IPAddress of the Ethernet/IP Device
        /// </summary>
        /// 
        public string IPAddress { get; set; } = "172.0.0.1";
        public string RoutePath
        {
            get
            {
                return _RoutePath;
            }
            set
            {
                _RoutePath = value;
                _useRoute = !string.IsNullOrWhiteSpace(_RoutePath);
                if (_useRoute)
                    _routebytes = PathToBytes(_RoutePath);
            }
        }
        private string _RoutePath;
        private byte[] _routebytes;
        private bool _useRoute;
        /// <summary>
        /// Requested Packet Rate (RPI) in Microseconds Originator -> Target for Implicit-Messaging (Default 0x7A120 -> 500ms)
        /// </summary>
        public UInt32 RequestedPacketRate_O_T { get; set; } = 0x7A120;      //500ms
        /// <summary>
        /// Requested Packet Rate (RPI) in Microseconds Target -> Originator for Implicit-Messaging (Default 0x7A120 -> 500ms)
        /// </summary>
        public UInt32 RequestedPacketRate_T_O { get; set; } = 0x7A120;      //500ms
        /// <summary>
        /// "1" Indicates that multiple connections are allowed Originator -> Target for Implicit-Messaging (Default: TRUE) 
        /// </summary>
        public bool O_T_OwnerRedundant { get; set; } = true;                //For Forward Open
        /// <summary>
        /// "1" Indicates that multiple connections are allowed Target -> Originator for Implicit-Messaging (Default: TRUE) 
        /// </summary>
        public bool T_O_OwnerRedundant { get; set; } = true;                //For Forward Open
        /// <summary>
        /// With a fixed size connection, the amount of data shall be the size of specified in the "Connection Size" Parameter.
        /// With a variable size, the amount of data could be up to the size specified in the "Connection Size" Parameter
        /// Originator -> Target for Implicit Messaging (Default: True (Variable length))
        /// </summary>
        public bool O_T_VariableLength { get; set; } = true;                //For Forward Open
        /// <summary>
        /// With a fixed size connection, the amount of data shall be the size of specified in the "Connection Size" Parameter.
        /// With a variable size, the amount of data could be up to the size specified in the "Connection Size" Parameter
        /// Target -> Originator for Implicit Messaging (Default: True (Variable length))
        /// </summary>
        public bool T_O_VariableLength { get; set; } = true;                //For Forward Open
        /// <summary>
        /// The maximum size in bytes (only pure data without sequence count and 32-Bit Real Time Header (if present)) from Originator -> Target for Implicit Messaging (Default: 505)
        /// </summary>
        public UInt16 O_T_Length { get; set; } = 505;                //For Forward Open - Max 505
        /// <summary>
        /// The maximum size in bytes (only pure data woithout sequence count and 32-Bit Real Time Header (if present)) from Target -> Originator for Implicit Messaging (Default: 505)
        /// </summary>
        public UInt16 T_O_Length { get; set; } = 505;                //For Forward Open - Max 505
        /// <summary>
        /// Connection Type Originator -> Target for Implicit Messaging (Default: ConnectionType.Point_to_Point)
        /// </summary>
        public ConnectionType O_T_ConnectionType { get; set; } = ConnectionType.Point_to_Point;
        /// <summary>
        /// Connection Type Target -> Originator for Implicit Messaging (Default: ConnectionType.Multicast)
        /// </summary>
        public ConnectionType T_O_ConnectionType { get; set; } = ConnectionType.Multicast;
        /// <summary>
        /// Priority Originator -> Target for Implicit Messaging (Default: Priority.Scheduled)
        /// Could be: Priority.Scheduled; Priority.High; Priority.Low; Priority.Urgent
        /// </summary>
        public Priority O_T_Priority { get; set; } = Priority.Scheduled;
        /// <summary>
        /// Priority Target -> Originator for Implicit Messaging (Default: Priority.Scheduled)
        /// Could be: Priority.Scheduled; Priority.High; Priority.Low; Priority.Urgent
        /// </summary>
        public Priority T_O_Priority { get; set; } = Priority.Scheduled;
        /// <summary>
        /// Class Assembly (Consuming IO-Path - Outputs) Originator -> Target for Implicit Messaging (Default: 0x64)
        /// </summary>
        public byte O_T_InstanceID { get; set; } = 0x64;               //Ausgänge
        /// <summary>
        /// Class Assembly (Producing IO-Path - Inputs) Target -> Originator for Implicit Messaging (Default: 0x64)
        /// </summary>
        public byte T_O_InstanceID { get; set; } = 0x65;               //Eingänge
        /// <summary>
        /// Provides Access to the Class 1 Real-Time IO-Data Originator -> Target for Implicit Messaging    
        /// </summary>
        public byte[] O_T_IOData = new byte[505];   //Class 1 Real-Time IO-Data O->T   
        /// <summary>
        /// Provides Access to the Class 1 Real-Time IO-Data Target -> Originator for Implicit Messaging
        /// </summary>
        public byte[] T_O_IOData = new byte[505];    //Class 1 Real-Time IO-Data T->O  
        /// <summary>
        /// Used Real-Time Format Originator -> Target for Implicit Messaging (Default: RealTimeFormat.Header32Bit)
        /// Possible Values: RealTimeFormat.Header32Bit; RealTimeFormat.Heartbeat; RealTimeFormat.ZeroLength; RealTimeFormat.Modeless
        /// </summary>
        public RealTimeFormat O_T_RealTimeFormat { get; set; } = RealTimeFormat.Header32Bit;
        /// <summary>
        /// Used Real-Time Format Target -> Originator for Implicit Messaging (Default: RealTimeFormat.Modeless)
        /// Possible Values: RealTimeFormat.Header32Bit; RealTimeFormat.Heartbeat; RealTimeFormat.ZeroLength; RealTimeFormat.Modeless
        /// </summary>
        public RealTimeFormat T_O_RealTimeFormat { get; set; } = RealTimeFormat.Modeless;
        /// <summary>
        /// AssemblyObject for the Configuration Path in case of Implicit Messaging (Standard: 0x04)
        /// </summary>
        public byte AssemblyObjectClass { get; set; } = 0x04;
        /// <summary>
        /// ConfigurationAssemblyInstanceID is the InstanceID of the configuration Instance in the Assembly Object Class (Standard: 0x01)
        /// </summary>
        public byte ConfigurationAssemblyInstanceID { get; set; } = 0x01;
        /// <summary>
        /// Returns the Date and Time when the last Implicit Message has been received fŕom The Target Device
        /// Could be used to determine a Timeout
        /// </summary>        
        public DateTime LastReceivedImplicitMessage { get; set; }
        //public TcpClient Client { get => client; set => client = value; }

        private static object _lock = new object();

        public ETHIPClient()
        {

        }

        private void ReceiveCallback(IAsyncResult ar)
        {
            lock (this)
            {
                UdpClient u = (UdpClient)((UdpState)(ar.AsyncState)).u;

                System.Net.IPEndPoint e = (System.Net.IPEndPoint)((UdpState)(ar.AsyncState)).e;

                Byte[] receiveBytes = u.EndReceive(ar, ref e);
                string receiveString = System.Text.Encoding.ASCII.GetString(receiveBytes);

                // EndReceive worked and we have received data and remote endpoint
                if (receiveBytes.Length > 0)
                {
                    UInt16 command = Convert.ToUInt16(receiveBytes[0]
                                                | (receiveBytes[1] << 8));
                    if (command == 0x63)
                    {
                        returnList.Add(Encapsulation.CIPIdentityItem.getCIPIdentityItem(24, receiveBytes));
                    }
                }
                var asyncResult = u.BeginReceive(new AsyncCallback(ReceiveCallback), (UdpState)(ar.AsyncState));
            }

        }
        public class UdpState
        {
            public System.Net.IPEndPoint e;
            public UdpClient u;

        }

        List<Encapsulation.CIPIdentityItem> returnList = new List<Encapsulation.CIPIdentityItem>();

        /// <summary>
        /// List and identify potential targets. This command shall be sent as braodcast massage using UDP.
        /// </summary>
        /// <returns>List<Encapsulation.CIPIdentityItem> contains the received informations from all devices </returns>	
        public List<Encapsulation.CIPIdentityItem> ListIdentity()
        {
            //ToDo: Beter use of Optix.Networkinterface... over System.Net.Network...
            foreach (System.Net.NetworkInformation.NetworkInterface ni in System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces())
            {
                if (ni.NetworkInterfaceType == System.Net.NetworkInformation.NetworkInterfaceType.Wireless80211 || ni.NetworkInterfaceType == System.Net.NetworkInformation.NetworkInterfaceType.Ethernet)
                {

                    foreach (UnicastIPAddressInformation ip in ni.GetIPProperties().UnicastAddresses)
                    {
                        if (ip.Address.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                        {
                            System.Net.IPAddress mask = ip.IPv4Mask;
                            System.Net.IPAddress address = ip.Address;

                            String multicastAddress = (address.GetAddressBytes()[0] | (~(mask.GetAddressBytes()[0])) & 0xFF).ToString() + "." + (address.GetAddressBytes()[1] | (~(mask.GetAddressBytes()[1])) & 0xFF).ToString() + "." + (address.GetAddressBytes()[2] | (~(mask.GetAddressBytes()[2])) & 0xFF).ToString() + "." + (address.GetAddressBytes()[3] | (~(mask.GetAddressBytes()[3])) & 0xFF).ToString();

                            byte[] sendData = new byte[24];
                            sendData[0] = 0x63;               //Command for "ListIdentity"
                            System.Net.Sockets.UdpClient udpClient = new System.Net.Sockets.UdpClient();
                            System.Net.IPEndPoint endPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Parse(multicastAddress), 44818);
                            udpClient.Send(sendData, sendData.Length, endPoint);

                            UdpState s = new UdpState();
                            s.e = endPoint;
                            s.u = udpClient;

                            var asyncResult = udpClient.BeginReceive(new AsyncCallback(ReceiveCallback), s);

                            System.Threading.Thread.Sleep(1000);
                        }
                    }
                }
            }
            return returnList;
        }

        /// <summary>
        /// Sends a RegisterSession command to a target to initiate session
        /// </summary>
        /// <param name="address">IP-Address of the target device</param> 
        /// <param name="port">Port of the target device (default should be 0xAF12)</param> 
        /// <returns>Session Handle</returns>	
        public UInt32 RegisterSession(UInt32 address, UInt16 port)
        {
            if (_sessionHandle != 0)
                return _sessionHandle;

            try
            {
                Encapsulation encapsulation = new Encapsulation();
                encapsulation.Command = Encapsulation.CommandsEnum.RegisterSession;
                encapsulation.Length = 4;
                encapsulation.CommandSpecificData.Add(1);       //Protocol version (should be set to 1)
                encapsulation.CommandSpecificData.Add(0);
                encapsulation.CommandSpecificData.Add(0);       //Session options shall be set to "0"
                encapsulation.CommandSpecificData.Add(0);


                string ipAddress = Encapsulation.CIPIdentityItem.getIPAddress(address);
                this.IPAddress = ipAddress;
                client = new TcpClient(ipAddress, port);
                stream = client.GetStream();
                client.ReceiveTimeout = 5000;
                client.ReceiveTimeout = 5000;
                stream.ReadTimeout = 5000;
                stream.WriteTimeout = 5000;

                stream.Write(encapsulation.toBytes(), 0, encapsulation.toBytes().Length);
                stream.Flush();
                byte[] data = new Byte[256];

                Int32 bytes = stream.Read(data, 0, data.Length);

                UInt32 returnvalue = (UInt32)data[4] + (((UInt32)data[5]) << 8) + (((UInt32)data[6]) << 16) + (((UInt32)data[7]) << 24);
                _sessionHandle = returnvalue;
                return returnvalue;
            }
            catch
            {
                if (_sessionHandle != 0)
                    throw;
                return 0;
            }
        }

        /// <summary>
        /// Sends a UnRegisterSession command to a target to terminate session
        /// </summary> 
        public void UnRegisterSession()
        {
            Encapsulation encapsulation = new Encapsulation();
            encapsulation.Command = Encapsulation.CommandsEnum.UnRegisterSession;
            encapsulation.Length = 0;
            encapsulation.SessionHandle = _sessionHandle;

            try
            {
                stream.Write(encapsulation.toBytes(), 0, encapsulation.toBytes().Length);
                stream.Flush();
            }
            catch (Exception)
            {
                //Handle Exception to allow to Close the Stream if the connection was closed by Remote Device
            }

            if (stream != null)
                stream.Close();
            if (client != null)
                client.Close();
            _sessionHandle = 0;
        }

        public void ForwardOpen()
        {
            this.ForwardOpen(false);
        }

        System.Net.Sockets.UdpClient udpClientReceive;
        bool udpClientReceiveClosed = false;
        public void ForwardOpen(bool largeForwardOpen)
        {
            udpClientReceiveClosed = false;
            ushort o_t_headerOffset = 2;                    //Zählt den Sequencecount und evtl 32bit header zu der Länge dazu
            if (O_T_RealTimeFormat == RealTimeFormat.Header32Bit)
                o_t_headerOffset = 6;
            if (O_T_RealTimeFormat == RealTimeFormat.Heartbeat)
                o_t_headerOffset = 0;

            ushort t_o_headerOffset = 2;                    //Zählt den Sequencecount und evtl 32bit header zu der Länge dazu
            if (T_O_RealTimeFormat == RealTimeFormat.Header32Bit)
                t_o_headerOffset = 6;
            if (T_O_RealTimeFormat == RealTimeFormat.Heartbeat)
                t_o_headerOffset = 0;

            int lengthOffset = (5 + (O_T_ConnectionType == ConnectionType.Null ? 0 : 2) + (T_O_ConnectionType == ConnectionType.Null ? 0 : 2));

            Encapsulation encapsulation = new Encapsulation();
            encapsulation.SessionHandle = _sessionHandle;
            encapsulation.Command = Encapsulation.CommandsEnum.SendRRData;
            //!!!!!!-----Length Field at the end!!!!!!!!!!!!!

            //---------------Interface Handle CIP
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Interface Handle CIP

            //----------------Timeout
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Timeout

            //Common Packet Format (Table 2-6.1)
            Encapsulation.CommonPacketFormat commonPacketFormat = new Encapsulation.CommonPacketFormat();
            commonPacketFormat.ItemCount = 0x02;

            commonPacketFormat.AddressItem = 0x0000;        //NULL (used for UCMM Messages)
            commonPacketFormat.AddressLength = 0x0000;


            commonPacketFormat.DataItem = 0xB2;
            commonPacketFormat.DataLength = (ushort)(41 + (ushort)lengthOffset);
            if (largeForwardOpen)
                commonPacketFormat.DataLength = (ushort)(commonPacketFormat.DataLength + 4);



            //----------------CIP Command "Forward Open" (Service Code 0x54)
            if (!largeForwardOpen)
                commonPacketFormat.Data.Add(0x54);
            //----------------CIP Command "Forward Open"  (Service Code 0x54)

            //----------------CIP Command "large Forward Open" (Service Code 0x5B)
            else
                commonPacketFormat.Data.Add(0x5B);
            //----------------CIP Command "large Forward Open"  (Service Code 0x5B)

            //----------------Requested Path size
            commonPacketFormat.Data.Add(2);
            //----------------Requested Path size

            //----------------Path segment for Class ID
            commonPacketFormat.Data.Add(0x20);
            commonPacketFormat.Data.Add((byte)6);
            //----------------Path segment for Class ID

            //----------------Path segment for Instance ID
            commonPacketFormat.Data.Add(0x24);
            commonPacketFormat.Data.Add((byte)1);
            //----------------Path segment for Instace ID

            //----------------Priority and Time/Tick - Table 3-5.16 (Vol. 1)
            commonPacketFormat.Data.Add(0x03);
            //----------------Priority and Time/Tick

            //----------------Timeout Ticks - Table 3-5.16 (Vol. 1)
            commonPacketFormat.Data.Add(0xfa);
            //----------------Timeout Ticks

            this.connectionID_O_T = Convert.ToUInt32(new Random().Next(0xfffffff));
            this.connectionID_T_O = Convert.ToUInt32(new Random().Next(0xfffffff) + 1);
            commonPacketFormat.Data.Add((byte)connectionID_O_T);
            commonPacketFormat.Data.Add((byte)(connectionID_O_T >> 8));
            commonPacketFormat.Data.Add((byte)(connectionID_O_T >> 16));
            commonPacketFormat.Data.Add((byte)(connectionID_O_T >> 24));


            commonPacketFormat.Data.Add((byte)connectionID_T_O);
            commonPacketFormat.Data.Add((byte)(connectionID_T_O >> 8));
            commonPacketFormat.Data.Add((byte)(connectionID_T_O >> 16));
            commonPacketFormat.Data.Add((byte)(connectionID_T_O >> 24));

            this.connectionSerialNumber = Convert.ToUInt16(new Random().Next(0xFFFF) + 2);
            commonPacketFormat.Data.Add((byte)connectionSerialNumber);
            commonPacketFormat.Data.Add((byte)(connectionSerialNumber >> 8));

            //----------------Originator Vendor ID
            commonPacketFormat.Data.Add(0xFF);
            commonPacketFormat.Data.Add(0);
            //----------------Originaator Vendor ID

            //----------------Originator Serial Number
            commonPacketFormat.Data.Add(0xFF);
            commonPacketFormat.Data.Add(0xFF);
            commonPacketFormat.Data.Add(0xFF);
            commonPacketFormat.Data.Add(0xFF);
            //----------------Originator Serial Number

            //----------------Timeout Multiplier
            commonPacketFormat.Data.Add(3);
            //----------------Timeout Multiplier

            //----------------Reserved
            commonPacketFormat.Data.Add(0);
            commonPacketFormat.Data.Add(0);
            commonPacketFormat.Data.Add(0);
            //----------------Reserved

            //----------------Requested Packet Rate O->T in Microseconds
            commonPacketFormat.Data.Add((byte)RequestedPacketRate_O_T);
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_O_T >> 8));
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_O_T >> 16));
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_O_T >> 24));
            //----------------Requested Packet Rate O->T in Microseconds

            //----------------O->T Network Connection Parameters
            bool redundantOwner = (bool)O_T_OwnerRedundant;
            byte connectionType = (byte)O_T_ConnectionType; //1=Multicast, 2=P2P
            byte priority = (byte)O_T_Priority;         //00=low; 01=High; 10=Scheduled; 11=Urgent
            bool variableLength = O_T_VariableLength;       //0=fixed; 1=variable
            UInt16 connectionSize = (ushort)(O_T_Length + o_t_headerOffset);      //The maximum size in bytes og the data for each direction (were applicable) of the connection. For a variable -> maximum
            UInt32 NetworkConnectionParameters = (UInt16)((UInt16)(connectionSize & 0x1FF) | ((Convert.ToUInt16(variableLength)) << 9) | ((priority & 0x03) << 10) | ((connectionType & 0x03) << 13) | ((Convert.ToUInt16(redundantOwner)) << 15));
            if (largeForwardOpen)
                NetworkConnectionParameters = (UInt32)((uint)(connectionSize & 0xFFFF) | ((Convert.ToUInt32(variableLength)) << 25) | (uint)((priority & 0x03) << 26) | (uint)((connectionType & 0x03) << 29) | ((Convert.ToUInt32(redundantOwner)) << 31));
            commonPacketFormat.Data.Add((byte)NetworkConnectionParameters);
            commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 8));
            if (largeForwardOpen)
            {
                commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 16));
                commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 24));
            }
            //----------------O->T Network Connection Parameters

            //----------------Requested Packet Rate T->O in Microseconds
            commonPacketFormat.Data.Add((byte)RequestedPacketRate_T_O);
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_T_O >> 8));
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_T_O >> 16));
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_T_O >> 24));
            //----------------Requested Packet Rate T->O in Microseconds

            //----------------T->O Network Connection Parameters


            redundantOwner = (bool)T_O_OwnerRedundant;
            connectionType = (byte)T_O_ConnectionType; //1=Multicast, 2=P2P
            priority = (byte)T_O_Priority;
            variableLength = T_O_VariableLength;
            connectionSize = (byte)(T_O_Length + t_o_headerOffset);
            NetworkConnectionParameters = (UInt16)((UInt16)(connectionSize & 0x1FF) | ((Convert.ToUInt16(variableLength)) << 9) | ((priority & 0x03) << 10) | ((connectionType & 0x03) << 13) | ((Convert.ToUInt16(redundantOwner)) << 15));
            if (largeForwardOpen)
                NetworkConnectionParameters = (UInt32)((uint)(connectionSize & 0xFFFF) | ((Convert.ToUInt32(variableLength)) << 25) | (uint)((priority & 0x03) << 26) | (uint)((connectionType & 0x03) << 29) | ((Convert.ToUInt32(redundantOwner)) << 31));
            commonPacketFormat.Data.Add((byte)NetworkConnectionParameters);
            commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 8));
            if (largeForwardOpen)
            {
                commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 16));
                commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 24));
            }
            //----------------T->O Network Connection Parameters

            //----------------Transport Type/Trigger
            commonPacketFormat.Data.Add(0x01);
            //X------- = 0= Client; 1= Server
            //-XXX---- = Production Trigger, 0 = Cyclic, 1 = CoS, 2 = Application Object
            //----XXXX = Transport class, 0 = Class 0, 1 = Class 1, 2 = Class 2, 3 = Class 3
            //----------------Transport Type Trigger
            //Connection Path size 
            commonPacketFormat.Data.Add((byte)((0x2) + (O_T_ConnectionType == ConnectionType.Null ? 0 : 1) + (T_O_ConnectionType == ConnectionType.Null ? 0 : 1)));
            //Verbindugspfad
            commonPacketFormat.Data.Add((byte)(0x20));
            commonPacketFormat.Data.Add((byte)(AssemblyObjectClass));
            commonPacketFormat.Data.Add((byte)(0x24));
            commonPacketFormat.Data.Add((byte)(ConfigurationAssemblyInstanceID));
            if (O_T_ConnectionType != ConnectionType.Null)
            {
                commonPacketFormat.Data.Add((byte)(0x2C));
                commonPacketFormat.Data.Add((byte)(O_T_InstanceID));
            }
            if (T_O_ConnectionType != ConnectionType.Null)
            {
                commonPacketFormat.Data.Add((byte)(0x2C));
                commonPacketFormat.Data.Add((byte)(T_O_InstanceID));
            }

            //AddSocket Addrress Item O->T

            commonPacketFormat.SocketaddrInfo_O_T = new Encapsulation.SocketAddress();
            commonPacketFormat.SocketaddrInfo_O_T.SIN_port = OriginatorUDPPort;
            commonPacketFormat.SocketaddrInfo_O_T.SIN_family = 2;
            if (O_T_ConnectionType == ConnectionType.Multicast)
            {
                UInt32 multicastResponseAddress = GetMulticastAddress(BitConverter.ToUInt32(System.Net.IPAddress.Parse(IPAddress).GetAddressBytes(), 0));

                commonPacketFormat.SocketaddrInfo_O_T.SIN_Address = (multicastResponseAddress);

                multicastAddress = commonPacketFormat.SocketaddrInfo_O_T.SIN_Address;
            }
            else
                commonPacketFormat.SocketaddrInfo_O_T.SIN_Address = 0;

            encapsulation.Length = (ushort)(commonPacketFormat.toBytes().Length + 6);//(ushort)(57 + (ushort)lengthOffset);
                                                                                     //20 04 24 01 2C 65 2C 6B

            byte[] dataToWrite = new byte[encapsulation.toBytes().Length + commonPacketFormat.toBytes().Length];
            System.Buffer.BlockCopy(encapsulation.toBytes(), 0, dataToWrite, 0, encapsulation.toBytes().Length);
            System.Buffer.BlockCopy(commonPacketFormat.toBytes(), 0, dataToWrite, encapsulation.toBytes().Length, commonPacketFormat.toBytes().Length);
            //encapsulation.toBytes();

            stream.Write(dataToWrite, 0, dataToWrite.Length);
            byte[] data = new Byte[564];

            Int32 bytes = stream.Read(data, 0, data.Length);

            //--------------------------BEGIN Error?
            if (data[42] != 0)      //Exception codes see "Table B-1.1 CIP General Status Codes"
            {
                if (data[42] == 0x1)
                    if (data[43] == 0)
                        throw new CIPException("Connection failure, General Status Code: " + data[42]);
                    else
                        throw new CIPException("Connection failure, General Status Code: " + data[42] + " Additional Status Code: " + ((data[45] << 8) | data[44]) + " " + GeneralStatusCodes.GetExtendedStatus((uint)((data[45] << 8) | data[44])));
                else
                    throw new CIPException(GeneralStatusCodes.GetStatusCode(data[42]));
            }
            //--------------------------END Error?
            //Read the Network ID from the Reply (see 3-3.7.1.1)
            int itemCount = data[30] + (data[31] << 8);
            int lengthUnconectedDataItem = data[38] + (data[39] << 8);
            this.connectionID_O_T = data[44] + (uint)(data[45] << 8) + (uint)(data[46] << 16) + (uint)(data[47] << 24);
            this.connectionID_T_O = data[48] + (uint)(data[49] << 8) + (uint)(data[50] << 16) + (uint)(data[51] << 24);

            //Is a SocketInfoItem present?
            int numberOfCurrentItem = 0;
            Encapsulation.SocketAddress socketInfoItem;
            while (itemCount > 2)
            {
                int typeID = data[40 + lengthUnconectedDataItem + 20 * numberOfCurrentItem] + (data[40 + lengthUnconectedDataItem + 1 + 20 * numberOfCurrentItem] << 8);
                if (typeID == 0x8001)
                {
                    socketInfoItem = new Encapsulation.SocketAddress();
                    socketInfoItem.SIN_Address = (UInt32)(data[40 + lengthUnconectedDataItem + 8 + 20 * numberOfCurrentItem]) + (UInt32)(data[40 + lengthUnconectedDataItem + 9 + 20 * numberOfCurrentItem] << 8) + (UInt32)(data[40 + lengthUnconectedDataItem + 10 + 20 * numberOfCurrentItem] << 16) + (UInt32)(data[40 + lengthUnconectedDataItem + 11 + 20 * numberOfCurrentItem] << 24);
                    socketInfoItem.SIN_port = (UInt16)((UInt16)(data[40 + lengthUnconectedDataItem + 7 + 20 * numberOfCurrentItem]) + (UInt16)(data[40 + lengthUnconectedDataItem + 6 + 20 * numberOfCurrentItem] << 8));
                    if (T_O_ConnectionType == ConnectionType.Multicast)
                        multicastAddress = socketInfoItem.SIN_Address;
                    TargetUDPPort = socketInfoItem.SIN_port;
                }
                numberOfCurrentItem++;
                itemCount--;
            }
            //Open UDP-Port



            System.Net.IPEndPoint endPointReceive = new System.Net.IPEndPoint(System.Net.IPAddress.Any, OriginatorUDPPort);
            udpClientReceive = new System.Net.Sockets.UdpClient(endPointReceive);
            UdpState s = new UdpState();
            s.e = endPointReceive;
            s.u = udpClientReceive;
            if (multicastAddress != 0)
            {
                System.Net.IPAddress multicast = (new System.Net.IPAddress(multicastAddress));
                udpClientReceive.JoinMulticastGroup(multicast);

            }

            System.Threading.Thread sendThread = new System.Threading.Thread(sendUDP);
            sendThread.Start();

            var asyncResult = udpClientReceive.BeginReceive(new AsyncCallback(ReceiveCallbackClass1), s);
        }
        public void ForwardOpenXXXTEST(bool largeForwardOpen)
        {                               //20 04 25 00 67 80 2c c7 2d 00 3d 04
                                        //byte[] test = new byte[] {0x5f, 0x02, 0x01, 0x80, 0x01, 0x00, 0x21, 0x00, 0x7f, 0x03, 0x24, 0x01, 0x04, 0x00, 0x34, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x20, 0x04, 0x25, 0x00, 0x69, 0x80, 0x2d, 0x00, 0x28, 0x80, 0x2d, 0x00, 0x53, 0x04};
                                        //20 04 25 00 69 80 2d 00 28 80 2d 00 53 04
                                        //byte[] test = new byte[] {0x20, 0x04, 0x25, 0x00, 0x69, 0x80, 0x2d, 0x00, 0x28, 0x80, 0x2d, 0x00, 0x53, 0x04};
                                        //byte[] test = new byte[] {0x20, 0x04, 0x25, 0x00, 0x67, 0x80, 0x2C, 0xC7, 0x2D, 0x00, 0x3D, 0x04};

            byte[] test = new byte[] { 0x20, 0x04, 0x25, 0x00, 0x67, 0x80, 0x2c, 0xc7, 0x2d, 0x00, 0x3d, 0x04, 0x80, 0x22, 0x03, 0x00, 0x03, 0x02, 0x02, 0x00, 0x02, 0x00, 0x02, 0x02, 0x02, 0x02, 0x02, 0x00, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00, 0x04, 0x04, 0x00, 0x00 };


            udpClientReceiveClosed = false;
            ushort o_t_headerOffset = 2;                    //Zählt den Sequencecount und evtl 32bit header zu der Länge dazu
            if (O_T_RealTimeFormat == RealTimeFormat.Header32Bit)
                o_t_headerOffset = 6;
            if (O_T_RealTimeFormat == RealTimeFormat.Heartbeat)
                o_t_headerOffset = 0;

            ushort t_o_headerOffset = 2;                    //Zählt den Sequencecount und evtl 32bit header zu der Länge dazu
            if (T_O_RealTimeFormat == RealTimeFormat.Header32Bit)
                t_o_headerOffset = 6;
            if (T_O_RealTimeFormat == RealTimeFormat.Heartbeat)
                t_o_headerOffset = 0;

            int lengthOffset = (5 + (O_T_ConnectionType == ConnectionType.Null ? 0 : 2) + (T_O_ConnectionType == ConnectionType.Null ? 0 : 2));

            Encapsulation encapsulation = new Encapsulation();
            encapsulation.SessionHandle = _sessionHandle;
            encapsulation.Command = Encapsulation.CommandsEnum.SendRRData;
            //!!!!!!-----Length Field at the end!!!!!!!!!!!!!

            //---------------Interface Handle CIP
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Interface Handle CIP

            //----------------Timeout
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Timeout

            //Common Packet Format (Table 2-6.1)
            Encapsulation.CommonPacketFormat commonPacketFormat = new Encapsulation.CommonPacketFormat();
            commonPacketFormat.ItemCount = 0x02;

            commonPacketFormat.AddressItem = 0x0000;        //NULL (used for UCMM Messages)
            commonPacketFormat.AddressLength = 0x0000;


            commonPacketFormat.DataItem = 0xB2;
            commonPacketFormat.DataLength = (ushort)(41 + test.Length - 4 + (ushort)lengthOffset);
            if (largeForwardOpen)
                commonPacketFormat.DataLength = (ushort)(commonPacketFormat.DataLength + 4);



            //----------------CIP Command "Forward Open" (Service Code 0x54)
            if (!largeForwardOpen)
                commonPacketFormat.Data.Add(0x54);
            //----------------CIP Command "Forward Open"  (Service Code 0x54)

            //----------------CIP Command "large Forward Open" (Service Code 0x5B)
            else
                commonPacketFormat.Data.Add(0x5B);
            //----------------CIP Command "large Forward Open"  (Service Code 0x5B)

            //----------------Requested Path size
            commonPacketFormat.Data.Add(2);
            //----------------Requested Path size

            //----------------Path segment for Class ID
            commonPacketFormat.Data.Add(0x20);
            commonPacketFormat.Data.Add((byte)6);
            //----------------Path segment for Class ID

            //----------------Path segment for Instance ID
            commonPacketFormat.Data.Add(0x24);
            commonPacketFormat.Data.Add((byte)1);
            //----------------Path segment for Instace ID

            //----------------Priority and Time/Tick - Table 3-5.16 (Vol. 1)
            commonPacketFormat.Data.Add(0x03);
            //----------------Priority and Time/Tick

            //----------------Timeout Ticks - Table 3-5.16 (Vol. 1)
            commonPacketFormat.Data.Add(0xfa);
            //----------------Timeout Ticks

            this.connectionID_O_T = Convert.ToUInt32(new Random().Next(0xfffffff));
            this.connectionID_T_O = Convert.ToUInt32(new Random().Next(0xfffffff) + 1);
            commonPacketFormat.Data.Add((byte)connectionID_O_T);
            commonPacketFormat.Data.Add((byte)(connectionID_O_T >> 8));
            commonPacketFormat.Data.Add((byte)(connectionID_O_T >> 16));
            commonPacketFormat.Data.Add((byte)(connectionID_O_T >> 24));


            commonPacketFormat.Data.Add((byte)connectionID_T_O);
            commonPacketFormat.Data.Add((byte)(connectionID_T_O >> 8));
            commonPacketFormat.Data.Add((byte)(connectionID_T_O >> 16));
            commonPacketFormat.Data.Add((byte)(connectionID_T_O >> 24));

            this.connectionSerialNumber = Convert.ToUInt16(new Random().Next(0xFFFF) + 2);
            commonPacketFormat.Data.Add((byte)connectionSerialNumber);
            commonPacketFormat.Data.Add((byte)(connectionSerialNumber >> 8));

            //----------------Originator Vendor ID
            commonPacketFormat.Data.Add(0x01);
            commonPacketFormat.Data.Add(0);
            //----------------Originaator Vendor ID

            //----------------Originator Serial Number
            commonPacketFormat.Data.Add(0xFF);
            commonPacketFormat.Data.Add(0xFF);
            commonPacketFormat.Data.Add(0xFF);
            commonPacketFormat.Data.Add(0xFF);
            //----------------Originator Serial Number

            //----------------Timeout Multiplier
            commonPacketFormat.Data.Add(3);
            //----------------Timeout Multiplier

            //----------------Reserved
            commonPacketFormat.Data.Add(0);
            commonPacketFormat.Data.Add(0);
            commonPacketFormat.Data.Add(0);
            //----------------Reserved

            //----------------Requested Packet Rate O->T in Microseconds
            commonPacketFormat.Data.Add((byte)RequestedPacketRate_O_T);
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_O_T >> 8));
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_O_T >> 16));
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_O_T >> 24));
            //----------------Requested Packet Rate O->T in Microseconds

            //----------------O->T Network Connection Parameters
            bool redundantOwner = (bool)O_T_OwnerRedundant;
            byte connectionType = (byte)O_T_ConnectionType; //1=Multicast, 2=P2P
            byte priority = (byte)O_T_Priority;         //00=low; 01=High; 10=Scheduled; 11=Urgent
            bool variableLength = O_T_VariableLength;       //0=fixed; 1=variable
            UInt16 connectionSize = (ushort)(O_T_Length + o_t_headerOffset);      //The maximum size in bytes og the data for each direction (were applicable) of the connection. For a variable -> maximum
            UInt32 NetworkConnectionParameters = (UInt16)((UInt16)(connectionSize & 0x1FF) | ((Convert.ToUInt16(variableLength)) << 9) | ((priority & 0x03) << 10) | ((connectionType & 0x03) << 13) | ((Convert.ToUInt16(redundantOwner)) << 15));
            if (largeForwardOpen)
                NetworkConnectionParameters = (UInt32)((uint)(connectionSize & 0xFFFF) | ((Convert.ToUInt32(variableLength)) << 25) | (uint)((priority & 0x03) << 26) | (uint)((connectionType & 0x03) << 29) | ((Convert.ToUInt32(redundantOwner)) << 31));
            commonPacketFormat.Data.Add((byte)NetworkConnectionParameters);
            commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 8));
            if (largeForwardOpen)
            {
                commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 16));
                commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 24));
            }
            //----------------O->T Network Connection Parameters

            //----------------Requested Packet Rate T->O in Microseconds
            commonPacketFormat.Data.Add((byte)RequestedPacketRate_T_O);
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_T_O >> 8));
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_T_O >> 16));
            commonPacketFormat.Data.Add((byte)(RequestedPacketRate_T_O >> 24));
            //----------------Requested Packet Rate T->O in Microseconds

            //----------------T->O Network Connection Parameters


            redundantOwner = (bool)T_O_OwnerRedundant;
            connectionType = (byte)T_O_ConnectionType; //1=Multicast, 2=P2P
            priority = (byte)T_O_Priority;
            variableLength = T_O_VariableLength;
            connectionSize = (byte)(T_O_Length + t_o_headerOffset);
            NetworkConnectionParameters = (UInt16)((UInt16)(connectionSize & 0x1FF) | ((Convert.ToUInt16(variableLength)) << 9) | ((priority & 0x03) << 10) | ((connectionType & 0x03) << 13) | ((Convert.ToUInt16(redundantOwner)) << 15));
            if (largeForwardOpen)
                NetworkConnectionParameters = (UInt32)((uint)(connectionSize & 0xFFFF) | ((Convert.ToUInt32(variableLength)) << 25) | (uint)((priority & 0x03) << 26) | (uint)((connectionType & 0x03) << 29) | ((Convert.ToUInt32(redundantOwner)) << 31));
            commonPacketFormat.Data.Add((byte)NetworkConnectionParameters);
            commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 8));
            if (largeForwardOpen)
            {
                commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 16));
                commonPacketFormat.Data.Add((byte)(NetworkConnectionParameters >> 24));
            }
            //----------------T->O Network Connection Parameters

            //----------------Transport Type/Trigger
            commonPacketFormat.Data.Add(0x01);
            //X------- = 0= Client; 1= Server
            //-XXX---- = Production Trigger, 0 = Cyclic, 1 = CoS, 2 = Application Object
            //----XXXX = Transport class, 0 = Class 0, 1 = Class 1, 2 = Class 2, 3 = Class 3
            //----------------Transport Type Trigger
            //Connection Path size 
            //---------------------------------HIER

            // commonPacketFormat.Data.Add((byte)((0x2) + (test.Length/2) + (O_T_ConnectionType == ConnectionType.Null ? 0 : 1) + (T_O_ConnectionType == ConnectionType.Null ? 0 : 1) ));
            // //Verbindugspfad
            // commonPacketFormat.Data.Add((byte)(0x20));
            // commonPacketFormat.Data.Add((byte)(AssemblyObjectClass));
            // commonPacketFormat.Data.Add((byte)(0x24));
            // commonPacketFormat.Data.Add((byte)(ConfigurationAssemblyInstanceID));
            commonPacketFormat.Data.Add((byte)((0) + (test.Length / 2) + (O_T_ConnectionType == ConnectionType.Null ? 0 : 1) + (T_O_ConnectionType == ConnectionType.Null ? 0 : 1)));
            for (int i = 0; i < test.Length; i++)
            {
                commonPacketFormat.Data.Add(test[i]);
            }
            //---------------------------------HIER
            if (O_T_ConnectionType != ConnectionType.Null)
            {
                commonPacketFormat.Data.Add((byte)(0x2C));
                commonPacketFormat.Data.Add((byte)(O_T_InstanceID));
            }
            if (T_O_ConnectionType != ConnectionType.Null)
            {
                commonPacketFormat.Data.Add((byte)(0x2C));
                commonPacketFormat.Data.Add((byte)(T_O_InstanceID));
            }

            //AddSocket Addrress Item O->T
            //????????????????            
            //     commonPacketFormat.SocketaddrInfo_O_T = new Encapsulation.SocketAddress();
            //     commonPacketFormat.SocketaddrInfo_O_T.SIN_port = OriginatorUDPPort;
            //     commonPacketFormat.SocketaddrInfo_O_T.SIN_family = 2;
            // if (O_T_ConnectionType == ConnectionType.Multicast)
            // {
            //     UInt32 multicastResponseAddress = ETHIPClient.GetMulticastAddress(BitConverter.ToUInt32(System.Net.IPAddress.Parse(IPAddress).GetAddressBytes(), 0));

            //     commonPacketFormat.SocketaddrInfo_O_T.SIN_Address = (multicastResponseAddress);

            //     multicastAddress = commonPacketFormat.SocketaddrInfo_O_T.SIN_Address;
            // }
            // else
            //     commonPacketFormat.SocketaddrInfo_O_T.SIN_Address = 0;

            encapsulation.Length = (ushort)(commonPacketFormat.toBytes().Length + 6);//(ushort)(57 + (ushort)lengthOffset);
                                                                                     //20 04 24 01 2C 65 2C 6B

            byte[] dataToWrite = new byte[encapsulation.toBytes().Length + commonPacketFormat.toBytes().Length];
            System.Buffer.BlockCopy(encapsulation.toBytes(), 0, dataToWrite, 0, encapsulation.toBytes().Length);
            System.Buffer.BlockCopy(commonPacketFormat.toBytes(), 0, dataToWrite, encapsulation.toBytes().Length, commonPacketFormat.toBytes().Length);
            //encapsulation.toBytes();

            stream.Write(dataToWrite, 0, dataToWrite.Length);
            stream.Flush();
            byte[] data = new Byte[564];

            Int32 bytes = stream.Read(data, 0, data.Length);

            //--------------------------BEGIN Error?
            if (data[42] != 0)      //Exception codes see "Table B-1.1 CIP General Status Codes"
            {
                if (data[42] == 0x1)
                    if (data[43] == 0)
                        throw new CIPException("Connection failure, General Status Code: " + data[42]);
                    else
                        throw new CIPException("Connection failure, General Status Code: " + data[42] + " Additional Status Code: " + ((data[45] << 8) | data[44]) + " " + GeneralStatusCodes.GetExtendedStatus((uint)((data[45] << 8) | data[44])));
                else
                    throw new CIPException(GeneralStatusCodes.GetStatusCode(data[42]));
            }
            //--------------------------END Error?
            //Read the Network ID from the Reply (see 3-3.7.1.1)
            //int itemCount = data[30] + (data[31] << 8);
            int lengthUnconectedDataItem = data[38] + (data[39] << 8);
            this.connectionID_O_T = data[44] + (uint)(data[45] << 8) + (uint)(data[46] << 16) + (uint)(data[47] << 24);
            this.connectionID_T_O = data[48] + (uint)(data[49] << 8) + (uint)(data[50] << 16) + (uint)(data[51] << 24);

            //Is a SocketInfoItem present?
            //-----------------------------hier
            // int numberOfCurrentItem = 0;
            // Encapsulation.SocketAddress socketInfoItem;
            // while (itemCount > 2)
            // {
            //     int typeID = data[40 + lengthUnconectedDataItem+ 20 * numberOfCurrentItem] + (data[40 + lengthUnconectedDataItem + 1+ 20 * numberOfCurrentItem] << 8);
            //     if (typeID == 0x8001)
            //     {
            //         socketInfoItem = new Encapsulation.SocketAddress();
            //         socketInfoItem.SIN_Address = (UInt32)(data[40 + lengthUnconectedDataItem + 8 + 20 * numberOfCurrentItem]) + (UInt32)(data[40 + lengthUnconectedDataItem + 9 + 20 * numberOfCurrentItem] << 8) + (UInt32)(data[40 + lengthUnconectedDataItem + 10 + 20 * numberOfCurrentItem] << 16) + (UInt32)(data[40 + lengthUnconectedDataItem + 11 + 20 * numberOfCurrentItem] << 24);
            //         socketInfoItem.SIN_port = (UInt16)((UInt16)(data[40 + lengthUnconectedDataItem + 7 + 20 * numberOfCurrentItem]) + (UInt16)(data[40 + lengthUnconectedDataItem + 6 + 20 * numberOfCurrentItem] << 8));
            //         if (T_O_ConnectionType == ConnectionType.Multicast)
            //             multicastAddress = socketInfoItem.SIN_Address;
            //         TargetUDPPort = socketInfoItem.SIN_port;
            //     }
            //     numberOfCurrentItem++;
            //     itemCount--;
            // }
            //Open UDP-Port



            System.Net.IPEndPoint endPointReceive = new System.Net.IPEndPoint(System.Net.IPAddress.Any, OriginatorUDPPort);
            udpClientReceive = new System.Net.Sockets.UdpClient(endPointReceive);
            UdpState s = new UdpState();
            s.e = endPointReceive;
            s.u = udpClientReceive;
            if (multicastAddress != 0)
            {
                System.Net.IPAddress multicast = (new System.Net.IPAddress(multicastAddress));
                udpClientReceive.JoinMulticastGroup(multicast);

            }

            System.Threading.Thread sendThread = new System.Threading.Thread(sendUDP);
            sendThread.Start();

            var asyncResult = udpClientReceive.BeginReceive(new AsyncCallback(ReceiveCallbackClass1), s);
        }

        public void LargeForwardOpen()
        {
            this.ForwardOpen(true);
        }

        private ushort o_t_detectedLength;
        /// <summary>
        /// Detects the Length of the data Originator -> Target.
        /// The Method uses an Explicit Message to detect the length.
        /// The IP-Address, Port and the Instance ID has to be defined before
        /// </summary>
        public ushort Detect_O_T_Length(int att = 3)
        {
            if (o_t_detectedLength == 0)
            {
                if (this._sessionHandle == 0)
                    this.RegisterSession();
                o_t_detectedLength = (ushort)(this.GetAttributeSingle(0x04, O_T_InstanceID, att)).Length;
                return o_t_detectedLength;
            }
            else
                return o_t_detectedLength;
        }

        private ushort t_o_detectedLength;
        /// <summary>
        /// Detects the Length of the data Target -> Originator.
        /// The Method uses an Explicit Message to detect the length.
        /// The IP-Address, Port and the Instance ID has to be defined before
        /// </summary>
        public ushort Detect_T_O_Length(int att = 3)
        {
            if (t_o_detectedLength == 0)
            {
                if (this._sessionHandle == 0)
                    this.RegisterSession();
                t_o_detectedLength = (ushort)(this.GetAttributeSingle(0x04, T_O_InstanceID, att)).Length;
                return t_o_detectedLength;
            }
            else
                return t_o_detectedLength;
        }

        private static UInt32 GetMulticastAddress(UInt32 deviceIPAddress)
        {
            UInt32 cip_Mcast_Base_Addr = 0xEFC00100;
            UInt32 cip_Host_Mask = 0x3FF;
            UInt32 netmask = 0;

            //Class A Network?
            if (deviceIPAddress <= 0x7FFFFFFF)
                netmask = 0xFF000000;
            //Class B Network?
            if (deviceIPAddress >= 0x80000000 && deviceIPAddress <= 0xBFFFFFFF)
                netmask = 0xFFFF0000;
            //Class C Network?
            if (deviceIPAddress >= 0xC0000000 && deviceIPAddress <= 0xDFFFFFFF)
                netmask = 0xFFFFFF00;

            UInt32 hostID = deviceIPAddress & ~netmask;
            UInt32 mcastIndex = hostID - 1;
            mcastIndex = mcastIndex & cip_Host_Mask;

            return (UInt32)(cip_Mcast_Base_Addr + mcastIndex * (UInt32)32);

        }

        public void ForwardClose()
        {
            //First stop the Thread which send data

            stopUDP = true;


            int lengthOffset = (5 + (O_T_ConnectionType == ConnectionType.Null ? 0 : 2) + (T_O_ConnectionType == ConnectionType.Null ? 0 : 2));

            Encapsulation encapsulation = new Encapsulation();
            encapsulation.SessionHandle = _sessionHandle;
            encapsulation.Command = Encapsulation.CommandsEnum.SendRRData;
            encapsulation.Length = (ushort)(16 + 17 + (ushort)lengthOffset);
            //---------------Interface Handle CIP
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Interface Handle CIP

            //----------------Timeout
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Timeout

            //Common Packet Format (Table 2-6.1)
            Encapsulation.CommonPacketFormat commonPacketFormat = new Encapsulation.CommonPacketFormat();
            commonPacketFormat.ItemCount = 0x02;

            commonPacketFormat.AddressItem = 0x0000;        //NULL (used for UCMM Messages)
            commonPacketFormat.AddressLength = 0x0000;


            commonPacketFormat.DataItem = 0xB2;
            commonPacketFormat.DataLength = (ushort)(17 + (ushort)lengthOffset);



            //----------------CIP Command "Forward Close"
            commonPacketFormat.Data.Add(0x4E);
            //----------------CIP Command "Forward Close"

            //----------------Requested Path size
            commonPacketFormat.Data.Add(2);
            //----------------Requested Path size

            //----------------Path segment for Class ID
            commonPacketFormat.Data.Add(0x20);
            commonPacketFormat.Data.Add((byte)6);
            //----------------Path segment for Class ID

            //----------------Path segment for Instance ID
            commonPacketFormat.Data.Add(0x24);
            commonPacketFormat.Data.Add((byte)1);
            //----------------Path segment for Instace ID

            //----------------Priority and Time/Tick - Table 3-5.16 (Vol. 1)
            commonPacketFormat.Data.Add(0x03);
            //----------------Priority and Time/Tick

            //----------------Timeout Ticks - Table 3-5.16 (Vol. 1)
            commonPacketFormat.Data.Add(0xfa);
            //----------------Timeout Ticks

            //Connection serial number
            commonPacketFormat.Data.Add((byte)connectionSerialNumber);
            commonPacketFormat.Data.Add((byte)(connectionSerialNumber >> 8));
            //connection seruial number

            //----------------Originator Vendor ID
            commonPacketFormat.Data.Add(0xFF);
            commonPacketFormat.Data.Add(0);
            //----------------Originaator Vendor ID

            //----------------Originator Serial Number
            commonPacketFormat.Data.Add(0xFF);
            commonPacketFormat.Data.Add(0xFF);
            commonPacketFormat.Data.Add(0xFF);
            commonPacketFormat.Data.Add(0xFF);
            //----------------Originator Serial Number

            //Connection Path size 
            commonPacketFormat.Data.Add((byte)((0x2) + (O_T_ConnectionType == ConnectionType.Null ? 0 : 1) + (T_O_ConnectionType == ConnectionType.Null ? 0 : 1)));
            //Reserved
            commonPacketFormat.Data.Add(0);
            //Reserved


            //Verbindugspfad
            commonPacketFormat.Data.Add((byte)(0x20));
            commonPacketFormat.Data.Add(AssemblyObjectClass);
            commonPacketFormat.Data.Add((byte)(0x24));
            commonPacketFormat.Data.Add((byte)(ConfigurationAssemblyInstanceID));
            if (O_T_ConnectionType != ConnectionType.Null)
            {
                commonPacketFormat.Data.Add((byte)(0x2C));
                commonPacketFormat.Data.Add((byte)(O_T_InstanceID));
            }
            if (T_O_ConnectionType != ConnectionType.Null)
            {
                commonPacketFormat.Data.Add((byte)(0x2C));
                commonPacketFormat.Data.Add((byte)(T_O_InstanceID));
            }

            byte[] dataToWrite = new byte[encapsulation.toBytes().Length + commonPacketFormat.toBytes().Length];
            System.Buffer.BlockCopy(encapsulation.toBytes(), 0, dataToWrite, 0, encapsulation.toBytes().Length);
            System.Buffer.BlockCopy(commonPacketFormat.toBytes(), 0, dataToWrite, encapsulation.toBytes().Length, commonPacketFormat.toBytes().Length);
            encapsulation.toBytes();
            try
            {
                stream.Write(dataToWrite, 0, dataToWrite.Length);
                stream.Flush();
            }
            catch (Exception e)
            {
                //Handle Exception  to allow Forward close if the connection was closed by the Remote Device before
                Console.WriteLine(e.Message);
            }
            byte[] data = new Byte[564];

            try
            {
                Int32 bytes = stream.Read(data, 0, data.Length);
            }
            catch (Exception e)
            {
                //Handle Exception  to allow Forward close if the connection was closed by the Remote Device before
                Console.WriteLine(e.Message);
            }


            //--------------------------BEGIN Error?
            if (data[42] != 0)      //Exception codes see "Table B-1.1 CIP General Status Codes"
            {
                throw new CIPException(GeneralStatusCodes.GetStatusCode(data[42]));
            }


            //Close the Socket for Receive
            udpClientReceiveClosed = true;
            if (udpClientReceive != null)
                udpClientReceive.Close();
        }

        private bool stopUDP;
        int sequence = 0;
        private void sendUDP()
        {
            System.Net.Sockets.UdpClient udpClientsend = new System.Net.Sockets.UdpClient();
            stopUDP = false;
            uint sequenceCount = 0;


            while (!stopUDP)
            {
                byte[] o_t_IOData = new byte[564];
                System.Net.IPEndPoint endPointsend = new System.Net.IPEndPoint(System.Net.IPAddress.Parse(IPAddress), TargetUDPPort);

                UdpState send = new UdpState();

                //---------------Item count
                o_t_IOData[0] = 2;
                o_t_IOData[1] = 0;
                //---------------Item count

                //---------------Type ID
                o_t_IOData[2] = 0x02;
                o_t_IOData[3] = 0x80;
                //---------------Type ID

                //---------------Length
                o_t_IOData[4] = 0x08;
                o_t_IOData[5] = 0x00;
                //---------------Length

                //---------------connection ID
                sequenceCount++;
                o_t_IOData[6] = (byte)(connectionID_O_T);
                o_t_IOData[7] = (byte)(connectionID_O_T >> 8);
                o_t_IOData[8] = (byte)(connectionID_O_T >> 16);
                o_t_IOData[9] = (byte)(connectionID_O_T >> 24);
                //---------------connection ID     

                //---------------sequence count
                o_t_IOData[10] = (byte)(sequenceCount);
                o_t_IOData[11] = (byte)(sequenceCount >> 8);
                o_t_IOData[12] = (byte)(sequenceCount >> 16);
                o_t_IOData[13] = (byte)(sequenceCount >> 24);
                //---------------sequence count            

                //---------------Type ID
                o_t_IOData[14] = 0xB1;
                o_t_IOData[15] = 0x00;
                //---------------Type ID

                ushort headerOffset = 0;
                if (O_T_RealTimeFormat == RealTimeFormat.Header32Bit)
                    headerOffset = 4;
                if (O_T_RealTimeFormat == RealTimeFormat.Heartbeat)
                    headerOffset = 0;
                ushort o_t_Length = (ushort)(O_T_Length + headerOffset + 2);   //Modeless and zero Length

                //---------------Length
                o_t_IOData[16] = (byte)o_t_Length;
                o_t_IOData[17] = (byte)(o_t_Length >> 8);
                //---------------Length

                //---------------Sequence count
                sequence++;
                if (O_T_RealTimeFormat != RealTimeFormat.Heartbeat)
                {
                    o_t_IOData[18] = (byte)sequence;
                    o_t_IOData[19] = (byte)(sequence >> 8);
                }
                //---------------Sequence count

                if (O_T_RealTimeFormat == RealTimeFormat.Header32Bit)
                {
                    o_t_IOData[20] = (byte)1;
                    o_t_IOData[21] = (byte)0;
                    o_t_IOData[22] = (byte)0;
                    o_t_IOData[23] = (byte)0;

                }

                //---------------Write data
                for (int i = 0; i < O_T_Length; i++)
                    o_t_IOData[20 + headerOffset + i] = (byte)O_T_IOData[i];
                //---------------Write data




                udpClientsend.Send(o_t_IOData, O_T_Length + 20 + headerOffset, endPointsend);
                System.Threading.Thread.Sleep((int)RequestedPacketRate_O_T / 1000);

            }

            udpClientsend.Close();

        }

        private void ReceiveCallbackClass1(IAsyncResult ar)
        {
            UdpClient u = (UdpClient)((UdpState)(ar.AsyncState)).u;
            if (udpClientReceiveClosed)
                return;

            u.BeginReceive(new AsyncCallback(ReceiveCallbackClass1), (UdpState)(ar.AsyncState));
            System.Net.IPEndPoint e = (System.Net.IPEndPoint)((UdpState)(ar.AsyncState)).e;


            Byte[] receiveBytes = u.EndReceive(ar, ref e);

            // EndReceive worked and we have received data and remote endpoint

            if (receiveBytes.Length > 20)
            {
                //Get the connection ID
                uint connectionID = (uint)(receiveBytes[6] | receiveBytes[7] << 8 | receiveBytes[8] << 16 | receiveBytes[9] << 24);


                if (connectionID == connectionID_T_O)
                {
                    ushort headerOffset = 0;
                    if (T_O_RealTimeFormat == RealTimeFormat.Header32Bit)
                        headerOffset = 4;
                    if (T_O_RealTimeFormat == RealTimeFormat.Heartbeat)
                        headerOffset = 0;
                    for (int i = 0; i < receiveBytes.Length - 20 - headerOffset; i++)
                    {
                        T_O_IOData[i] = receiveBytes[20 + i + headerOffset];
                    }
                    //Console.WriteLine(T_O_IOData[0]);


                }
            }
            LastReceivedImplicitMessage = DateTime.Now;
        }



        /// <summary>
        /// Sends a RegisterSession command to a target to initiate session
        /// </summary>
        /// <param name="address">IP-Address of the target device</param> 
        /// <param name="port">Port of the target device (default should be 0xAF12)</param> 
        /// <returns>Session Handle</returns>	
        public UInt32 RegisterSession(string address, UInt16 port)
        {
            string[] addressSubstring = address.Split('.');
            UInt32 ipAddress = UInt32.Parse(addressSubstring[3]) + (UInt32.Parse(addressSubstring[2]) << 8) + (UInt32.Parse(addressSubstring[1]) << 16) + (UInt32.Parse(addressSubstring[0]) << 24);
            return RegisterSession(ipAddress, port);
        }

        /// <summary>
        /// Sends a RegisterSession command to a target to initiate session with the Standard or predefined Port (Standard: 0xAF12)
        /// </summary>
        /// <param name="address">IP-Address of the target device</param> 
        /// <returns>Session Handle</returns>	
        public UInt32 RegisterSession(string address)
        {
            string[] addressSubstring = address.Split('.');
            UInt32 ipAddress = UInt32.Parse(addressSubstring[3]) + (UInt32.Parse(addressSubstring[2]) << 8) + (UInt32.Parse(addressSubstring[1]) << 16) + (UInt32.Parse(addressSubstring[0]) << 24);
            return RegisterSession(ipAddress, this.TCPPort);
        }

        /// <summary>
        /// Sends a RegisterSession command to a target to initiate session with the Standard or predefined Port and Predefined IPAddress (Standard-Port: 0xAF12)
        /// </summary>
        /// <returns>Session Handle</returns>	
        public UInt32 RegisterSession()
        {

            return RegisterSession(this.IPAddress, this.TCPPort);
        }

        public byte[] GetAttributeSingle(int classID, int instanceID, int attributeID)
        {
            byte[] requestedPath = GetEPath(classID, instanceID, attributeID);
            if (_sessionHandle == 0)             //If a Session is not Registers, Try to Registers a Session with the predefined IP-Address and Port
                this.RegisterSession();

            byte[] dataToSend = new byte[42 + requestedPath.Length];
            Encapsulation encapsulation = new Encapsulation();
            encapsulation.SessionHandle = _sessionHandle;
            encapsulation.Command = Encapsulation.CommandsEnum.SendRRData;
            encapsulation.Length = _useRoute ? (UInt16)(requestedPath.Length + 18 + 12 + _routebytes.Length) : (UInt16)(18 + requestedPath.Length);
            //---------------Interface Handle CIP
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Interface Handle CIP

            //----------------Timeout
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Timeout

            //Common Packet Format (Table 2-6.1)
            Encapsulation.CommonPacketFormat commonPacketFormat = new Encapsulation.CommonPacketFormat();
            commonPacketFormat.ItemCount = 0x02;

            commonPacketFormat.AddressItem = 0x0000;        //NULL (used for UCMM Messages)
            commonPacketFormat.AddressLength = 0x0000;

            commonPacketFormat.DataItem = 0xB2;
            commonPacketFormat.DataLength = _useRoute ? (UInt16)(2 + requestedPath.Length + 12 + _routebytes.Length) : (UInt16)(2 + requestedPath.Length);

            if (_useRoute)
            {
                commonPacketFormat.Data.Add((byte)0x52); // Service: Connection Manager foreward
                commonPacketFormat.Data.Add((byte)0x2); // Path length
                commonPacketFormat.Data.Add((byte)0x20); // Path Segment1
                commonPacketFormat.Data.Add((byte)0x06); // Path Segment1
                commonPacketFormat.Data.Add((byte)0x24); // Path Segment2
                commonPacketFormat.Data.Add((byte)0x01); // Path Segment2
                                                         //Time Out
                                                         //commonPacketFormat.Data.AddRange(BitConverter.GetBytes((UInt16)(10000)));
                commonPacketFormat.Data.Add(0x06);
                commonPacketFormat.Data.Add(0x9B);
                commonPacketFormat.Data.AddRange(BitConverter.GetBytes((UInt16)(2 + requestedPath.Length)));
            }

            //----------------CIP Command "Get Attribute Single"
            commonPacketFormat.Data.Add((byte)CIPServices.Get_Attribute_Single);
            //----------------CIP Command "Get Attribute Single"

            //----------------Requested Path size (number of 16 bit words)
            commonPacketFormat.Data.Add((byte)(requestedPath.Length / 2));
            //----------------Requested Path size (number of 16 bit words)

            //----------------Path segment for Class ID
            //----------------Path segment for Class ID

            //----------------Path segment for Instance ID
            //----------------Path segment for Instace ID

            //----------------Path segment for Attribute ID
            //----------------Path segment for Attribute ID

            for (int i = 0; i < requestedPath.Length; i++)
            {
                commonPacketFormat.Data.Add(requestedPath[i]);
            }

            if (_useRoute)
            {
                commonPacketFormat.Data.Add((byte)(_routebytes.Length / 2));
                commonPacketFormat.Data.Add((byte)0);

                commonPacketFormat.Data.AddRange(_routebytes);
            }

            byte[] dataToWrite = new byte[encapsulation.toBytes().Length + commonPacketFormat.toBytes().Length];
            System.Buffer.BlockCopy(encapsulation.toBytes(), 0, dataToWrite, 0, encapsulation.toBytes().Length);
            System.Buffer.BlockCopy(commonPacketFormat.toBytes(), 0, dataToWrite, encapsulation.toBytes().Length, commonPacketFormat.toBytes().Length);
            //encapsulation.toBytes();

            //stream.Write(dataToWrite, 0, dataToWrite.Length);
            //stream.Flush();
            //byte[] data = new Byte[client.ReceiveBufferSize];

            //Int32 bytes = stream.Read(data, 0, data.Length);
            byte[] data;
            Int32 bytes = sendRequest(dataToWrite, out data);

            if (bytes < 44)
                throw new CIPException("Not enough data returned");
            //--------------------------BEGIN Error?
            if (data[42] != 0)      //Exception codes see "Table B-1.1 CIP General Status Codes"
            {
                throw new CIPException(GeneralStatusCodes.GetStatusCode(data[42]));
            }

            //--------------------------END Error?
            byte[] returnData = new byte[bytes - 44];

            System.Buffer.BlockCopy(data, 44, returnData, 0, bytes - 44);

            return returnData;
        }

        private byte[] PathToBytes(string ETHIPPath)
        {
            List<byte> res = new List<byte>();
            string[] segments = ETHIPPath.Split(",");
            if (segments.Length % 2 != 0)
                throw new Exception("Invalid Path: " + ETHIPPath);

            for (int i = 0; i < segments.Length; i += 2)
            {
                byte media = byte.Parse(segments[i]);

                if ((media < 1) | (media > 3))
                    throw new Exception("Unsupported media type in Path: " + media.ToString());

                switch (media)
                {
                    case 1: //Backplane
                        res.Add(media);
                        byte port = byte.Parse(segments[i + 1]);
                        res.Add(port);
                        break;
                    case 2:
                    case 3:
                    case 4: //Fieldbud, e.g. Ethernet
                        res.Add((byte)(0x10 + media));
                        res.Add((byte)segments[i + 1].Length);
                        res.AddRange(ASCIIEncoding.ASCII.GetBytes(segments[i + 1]));
                        if (segments[i + 1].Length % 2 != 0)
                            res.Add(0);
                        break;
                    //case int n when (n < 99 && n >= 1 ):

                    default:
                        throw new Exception("Unsupported media type in Path: " + media.ToString());
                }
            }

            byte[] resbytes = new byte[res.Count];
            for (int i = 0; i < res.Count; i++)
            {
                resbytes[i] = res[i];
            }
            return resbytes;
        }

        public byte[] CustomService(int ServiceCode, int classID, int instanceID, int attributeID, byte[] DataToSend)
        {
            byte[] requestedPath = GetEPath(classID, instanceID, attributeID);
            if (_sessionHandle == 0)             //If a Session is not Registers, Try to Registers a Session with the predefined IP-Address and Port
                this.RegisterSession();
            byte[] dataToSend = new byte[42 + DataToSend.Length + requestedPath.Length];
            Encapsulation encapsulation = new Encapsulation();
            encapsulation.SessionHandle = _sessionHandle;
            encapsulation.Command = Encapsulation.CommandsEnum.SendRRData;
            encapsulation.Length = _useRoute ? (UInt16)(requestedPath.Length + 18 + 12 + DataToSend.Length + _routebytes.Length) : (UInt16)(18 + DataToSend.Length + requestedPath.Length);
            //---------------Interface Handle CIP
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Interface Handle CIP

            //----------------Timeout
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Timeout

            //Common Packet Format (Table 2-6.1)
            Encapsulation.CommonPacketFormat commonPacketFormat = new Encapsulation.CommonPacketFormat();
            commonPacketFormat.ItemCount = 0x02;

            commonPacketFormat.AddressItem = 0x0000;        //NULL (used for UCMM Messages)
            commonPacketFormat.AddressLength = 0x0000;

            commonPacketFormat.DataItem = 0xB2;
            commonPacketFormat.DataLength = _useRoute ? (UInt16)(2 + requestedPath.Length + 12 + DataToSend.Length + _routebytes.Length) : (UInt16)(2 + DataToSend.Length + requestedPath.Length);

            if (_useRoute)
            {
                commonPacketFormat.Data.Add((byte)0x52); // Service: Connection Manager foreward
                commonPacketFormat.Data.Add((byte)0x2); // Path length
                commonPacketFormat.Data.Add((byte)0x20); // Path Segment1
                commonPacketFormat.Data.Add((byte)0x06); // Path Segment1
                commonPacketFormat.Data.Add((byte)0x24); // Path Segment2
                commonPacketFormat.Data.Add((byte)0x01); // Path Segment2
                                                         //Time Out
                commonPacketFormat.Data.AddRange(BitConverter.GetBytes((UInt16)(10000)));
                commonPacketFormat.Data.AddRange(BitConverter.GetBytes((UInt16)(2 + requestedPath.Length + DataToSend.Length)));
            }

            //----------------CIP Command "Set Attribute Single"
            commonPacketFormat.Data.Add((byte)ServiceCode);
            //----------------CIP Command "Set Attribute Single"

            //----------------Requested Path size (number of 16 bit words)
            commonPacketFormat.Data.Add((byte)(requestedPath.Length / 2));
            //----------------Requested Path size (number of 16 bit words)

            //----------------Path segment for Class ID
            //----------------Path segment for Class ID

            //----------------Path segment for Instance ID
            //----------------Path segment for Instace ID

            //----------------Path segment for Attribute ID
            //----------------Path segment for Attribute ID
            for (int i = 0; i < requestedPath.Length; i++)
            {
                commonPacketFormat.Data.Add(requestedPath[i]);
            }

            //----------------Data
            for (int i = 0; i < DataToSend.Length; i++)
            {
                commonPacketFormat.Data.Add(DataToSend[i]);
            }
            //----------------Data

            if (_useRoute)
            {
                commonPacketFormat.Data.Add((byte)(_routebytes.Length / 2));
                commonPacketFormat.Data.Add((byte)0);

                commonPacketFormat.Data.AddRange(_routebytes);
            }

            byte[] dataToWrite = new byte[encapsulation.toBytes().Length + commonPacketFormat.toBytes().Length];
            System.Buffer.BlockCopy(encapsulation.toBytes(), 0, dataToWrite, 0, encapsulation.toBytes().Length);
            System.Buffer.BlockCopy(commonPacketFormat.toBytes(), 0, dataToWrite, encapsulation.toBytes().Length, commonPacketFormat.toBytes().Length);
            //encapsulation.toBytes();

            //stream.Write(dataToWrite, 0, dataToWrite.Length);
            //stream.Flush();
            //byte[] data = new Byte[client.ReceiveBufferSize];

            //Int32 bytes = stream.Read(data, 0, data.Length);

            byte[] data;
            Int32 bytes = sendRequest(dataToWrite, out data);

            //--------------------------BEGIN Error?
            if (data[42] != 0)      //Exception codes see "Table B-1.1 CIP General Status Codes"
            {
                throw new CIPException(GeneralStatusCodes.GetStatusCode(data[42]));
            }
            //--------------------------END Error?

            byte[] returnData = new byte[bytes - 44];
            System.Buffer.BlockCopy(data, 44, returnData, 0, bytes - 44);

            return returnData;
        }

        /// <summary>
        /// Implementation of Common Service "Get_Attribute_All" - Service Code: 0x01
        /// </summary>
        /// <param name="classID">Class id of requested Attributes</param> 
        /// <param name="instanceID">Instance of Requested Attributes (0 for class Attributes)</param> 
        /// <returns>Session Handle</returns>	
        public BinaryReader GetAttributeAll(int classID, int instanceID)
        {
            byte[] requestedPath = GetEPath(classID, instanceID, 0);
            if (_sessionHandle == 0)             //If a Session is not Registered, Try to Registers a Session with the predefined IP-Address and Port
                this.RegisterSession();
            byte[] dataToSend = new byte[42 + requestedPath.Length];
            Encapsulation encapsulation = new Encapsulation();
            encapsulation.SessionHandle = _sessionHandle;
            encapsulation.Command = Encapsulation.CommandsEnum.SendRRData;
            encapsulation.Length = _useRoute ? (UInt16)(requestedPath.Length + 18 + 12 + _routebytes.Length) : (UInt16)(18 + requestedPath.Length);
            //---------------Interface Handle CIP
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Interface Handle CIP

            //----------------Timeout
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Timeout

            //Common Packet Format (Table 2-6.1)
            //Common Packet Format (Table 2-6.1)
            Encapsulation.CommonPacketFormat commonPacketFormat = new Encapsulation.CommonPacketFormat();
            commonPacketFormat.ItemCount = 0x02;

            commonPacketFormat.AddressItem = 0x0000;        //NULL (used for UCMM Messages)
            commonPacketFormat.AddressLength = 0x0000;

            commonPacketFormat.DataItem = 0xB2;
            commonPacketFormat.DataLength = _useRoute ? (UInt16)(2 + requestedPath.Length + 12 + _routebytes.Length) : (UInt16)(2 + requestedPath.Length);

            if (_useRoute)
            {
                commonPacketFormat.Data.Add((byte)0x52); // Service: Connection Manager foreward
                commonPacketFormat.Data.Add((byte)0x2); // Path length
                commonPacketFormat.Data.Add((byte)0x20); // Path Segment1
                commonPacketFormat.Data.Add((byte)0x06); // Path Segment1
                commonPacketFormat.Data.Add((byte)0x24); // Path Segment2
                commonPacketFormat.Data.Add((byte)0x01); // Path Segment2
                                                         //Time Out
                                                         //commonPacketFormat.Data.AddRange(BitConverter.GetBytes((UInt16)(10000)));
                commonPacketFormat.Data.Add(0x06);
                commonPacketFormat.Data.Add(0x9B);
                commonPacketFormat.Data.AddRange(BitConverter.GetBytes((UInt16)(2 + requestedPath.Length)));
            }

            //----------------CIP Command "Get Attribute All"
            commonPacketFormat.Data.Add((byte)CIPServices.Get_Attributes_All);
            //----------------CIP Command "Get Attribute All"

            //----------------Requested Path size
            commonPacketFormat.Data.Add((byte)(requestedPath.Length / 2));
            //----------------Requested Path size

            //----------------Path segment for Class ID
            //----------------Path segment for Class ID

            //----------------Path segment for Instance ID
            //----------------Path segment for Instace ID
            for (int i = 0; i < requestedPath.Length; i++)
            {
                commonPacketFormat.Data.Add(requestedPath[i]);
            }

            if (_useRoute)
            {
                commonPacketFormat.Data.Add((byte)(_routebytes.Length / 2));
                commonPacketFormat.Data.Add((byte)0);

                commonPacketFormat.Data.AddRange(_routebytes);
            }

            byte[] dataToWrite = new byte[encapsulation.toBytes().Length + commonPacketFormat.toBytes().Length];
            System.Buffer.BlockCopy(encapsulation.toBytes(), 0, dataToWrite, 0, encapsulation.toBytes().Length);
            System.Buffer.BlockCopy(commonPacketFormat.toBytes(), 0, dataToWrite, encapsulation.toBytes().Length, commonPacketFormat.toBytes().Length);


            //stream.Write(dataToWrite, 0, dataToWrite.Length);
            //stream.Flush();
            //byte[] data = new Byte[client.ReceiveBufferSize];

            //Int32 bytes = stream.Read(data, 0, data.Length);
            byte[] data;
            Int32 bytes = sendRequest(dataToWrite, out data);
            //--------------------------BEGIN Error?
            if (data[42] != 0)      //Exception codes see "Table B-1.1 CIP General Status Codes"
            {
                throw new CIPException(GeneralStatusCodes.GetStatusCode(data[42]));
            }
            //--------------------------END Error?

            byte[] returnData = new byte[bytes - 44];
            System.Buffer.BlockCopy(data, 44, returnData, 0, bytes - 44);

            return new BinaryReader(new MemoryStream(returnData));
        }

        public byte[] GetAttributeAllAsBytes(int classID, int instanceID)
        {
            byte[] requestedPath = GetEPath(classID, instanceID, 0);
            if (_sessionHandle == 0)             //If a Session is not Registered, Try to Registers a Session with the predefined IP-Address and Port
                this.RegisterSession();
            byte[] dataToSend = new byte[42 + requestedPath.Length];
            Encapsulation encapsulation = new Encapsulation();
            encapsulation.SessionHandle = _sessionHandle;
            encapsulation.Command = Encapsulation.CommandsEnum.SendRRData;
            encapsulation.Length = _useRoute ? (UInt16)(requestedPath.Length + 18 + 12 + _routebytes.Length) : (UInt16)(18 + requestedPath.Length);
            //---------------Interface Handle CIP
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Interface Handle CIP

            //----------------Timeout
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Timeout

            //Common Packet Format (Table 2-6.1)
            Encapsulation.CommonPacketFormat commonPacketFormat = new Encapsulation.CommonPacketFormat();
            commonPacketFormat.ItemCount = 0x02;

            commonPacketFormat.AddressItem = 0x0000;        //NULL (used for UCMM Messages)
            commonPacketFormat.AddressLength = 0x0000;

            commonPacketFormat.DataItem = 0xB2;
            commonPacketFormat.DataLength = _useRoute ? (UInt16)(2 + requestedPath.Length + 12 + _routebytes.Length) : (UInt16)(2 + requestedPath.Length);

            if (_useRoute)
            {
                commonPacketFormat.Data.Add((byte)0x52); // Service: Connection Manager foreward
                commonPacketFormat.Data.Add((byte)0x2); // Path length
                commonPacketFormat.Data.Add((byte)0x20); // Path Segment1
                commonPacketFormat.Data.Add((byte)0x06); // Path Segment1
                commonPacketFormat.Data.Add((byte)0x24); // Path Segment2
                commonPacketFormat.Data.Add((byte)0x01); // Path Segment2
                                                         //Time Out
                commonPacketFormat.Data.AddRange(BitConverter.GetBytes((UInt16)(10000)));
                commonPacketFormat.Data.AddRange(BitConverter.GetBytes((UInt16)(2 + requestedPath.Length)));
            }

            //----------------CIP Command "Get Attribute All"
            commonPacketFormat.Data.Add((byte)CIPServices.Get_Attributes_All);
            //----------------CIP Command "Get Attribute All"

            //----------------Requested Path size
            commonPacketFormat.Data.Add((byte)(requestedPath.Length / 2));
            //----------------Requested Path size

            //----------------Path segment for Class ID
            //----------------Path segment for Class ID

            //----------------Path segment for Instance ID
            //----------------Path segment for Instace ID
            for (int i = 0; i < requestedPath.Length; i++)
            {
                commonPacketFormat.Data.Add(requestedPath[i]);
            }

            if (_useRoute)
            {
                commonPacketFormat.Data.Add((byte)(_routebytes.Length / 2));
                commonPacketFormat.Data.Add((byte)0);

                commonPacketFormat.Data.AddRange(_routebytes);
            }

            byte[] dataToWrite = new byte[encapsulation.toBytes().Length + commonPacketFormat.toBytes().Length];
            System.Buffer.BlockCopy(encapsulation.toBytes(), 0, dataToWrite, 0, encapsulation.toBytes().Length);
            System.Buffer.BlockCopy(commonPacketFormat.toBytes(), 0, dataToWrite, encapsulation.toBytes().Length, commonPacketFormat.toBytes().Length);


            //stream.Write(dataToWrite, 0, dataToWrite.Length);
            //stream.Flush();
            //byte[] data = new Byte[client.ReceiveBufferSize];

            //Int32 bytes = stream.Read(data, 0, data.Length);
            byte[] data;
            Int32 bytes = sendRequest(dataToWrite, out data);
            //--------------------------BEGIN Error?
            if (data[42] != 0)      //Exception codes see "Table B-1.1 CIP General Status Codes"
            {
                throw new CIPException(GeneralStatusCodes.GetStatusCode(data[42]));
            }
            //--------------------------END Error?

            byte[] returnData = new byte[bytes - 44];
            System.Buffer.BlockCopy(data, 44, returnData, 0, bytes - 44);

            return returnData;
        }

        private Int32 sendRequest(byte[] dataToWrite, out byte[] dataRecieved)
        {
            try
            {
                Int32 bytes = 0;
                byte[] data = new Byte[client.ReceiveBufferSize];

                lock (_lock)
                {
                    stream.Write(dataToWrite, 0, dataToWrite.Length);
                    stream.Flush();

                    bytes = stream.Read(data, 0, data.Length);
                }
                dataRecieved = data;
                return bytes;
            }
            catch (Exception ex)
            {
                if (_sessionHandle != 0)
                    Log.Error("Connection to " + IPAddress + ":" + TCPPort + " on Route'" + RoutePath + "' failed." + Environment.NewLine + ex.Message + Environment.NewLine + ex.StackTrace);
                _sessionHandle = 0;
                dataRecieved = new byte[0];
                return 0;
            }
        }

        public Encapsulation.CommonPacketFormat GetAttributeAll_CommandData(int classID, int instanceID)
        {
            byte[] requestedPath = GetEPath(classID, instanceID, 0);

            //Common Packet Format (Table 2-6.1)
            Encapsulation.CommonPacketFormat commonPacketFormat = new Encapsulation.CommonPacketFormat();
            commonPacketFormat.ItemCount = 0x02;

            commonPacketFormat.AddressItem = 0x0000;        //NULL (used for UCMM Messages)
            commonPacketFormat.AddressLength = 0x0000;

            commonPacketFormat.DataItem = 0xB2;
            commonPacketFormat.DataLength = (UInt16)(2 + requestedPath.Length); //WAS 6



            //----------------CIP Command "Get Attribute All"
            commonPacketFormat.Data.Add((byte)CIPServices.Get_Attributes_All);
            //----------------CIP Command "Get Attribute All"

            //----------------Requested Path size
            commonPacketFormat.Data.Add((byte)(requestedPath.Length / 2));
            //----------------Requested Path size

            //----------------Path segment for Class ID
            //----------------Path segment for Class ID

            //----------------Path segment for Instance ID
            //----------------Path segment for Instace ID
            for (int i = 0; i < requestedPath.Length; i++)
            {
                commonPacketFormat.Data.Add(requestedPath[i]);
            }

            return commonPacketFormat;
        }

        public byte[] SetAttributeSingle(int classID, int instanceID, int attributeID, byte[] value)
        {
            byte[] requestedPath = GetEPath(classID, instanceID, attributeID);
            if (_sessionHandle == 0)             //If a Session is not Registers, Try to Registers a Session with the predefined IP-Address and Port
                this.RegisterSession();
            byte[] dataToSend = new byte[42 + value.Length + requestedPath.Length];
            Encapsulation encapsulation = new Encapsulation();
            encapsulation.SessionHandle = _sessionHandle;
            encapsulation.Command = Encapsulation.CommandsEnum.SendRRData;
            encapsulation.Length = (UInt16)(18 + value.Length + requestedPath.Length);
            //---------------Interface Handle CIP
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Interface Handle CIP

            //----------------Timeout
            encapsulation.CommandSpecificData.Add(0);
            encapsulation.CommandSpecificData.Add(0);
            //----------------Timeout

            //Common Packet Format (Table 2-6.1)
            Encapsulation.CommonPacketFormat commonPacketFormat = new Encapsulation.CommonPacketFormat();
            commonPacketFormat.ItemCount = 0x02;

            commonPacketFormat.AddressItem = 0x0000;        //NULL (used for UCMM Messages)
            commonPacketFormat.AddressLength = 0x0000;

            commonPacketFormat.DataItem = 0xB2;
            commonPacketFormat.DataLength = (UInt16)(2 + value.Length + requestedPath.Length);



            //----------------CIP Command "Set Attribute Single"
            commonPacketFormat.Data.Add((byte)CIPServices.Set_Attribute_Single);
            //----------------CIP Command "Set Attribute Single"

            //----------------Requested Path size (number of 16 bit words)
            commonPacketFormat.Data.Add((byte)(requestedPath.Length / 2));
            //----------------Requested Path size (number of 16 bit words)

            //----------------Path segment for Class ID
            //----------------Path segment for Class ID

            //----------------Path segment for Instance ID
            //----------------Path segment for Instace ID

            //----------------Path segment for Attribute ID
            //----------------Path segment for Attribute ID
            for (int i = 0; i < requestedPath.Length; i++)
            {
                commonPacketFormat.Data.Add(requestedPath[i]);
            }

            //----------------Data
            for (int i = 0; i < value.Length; i++)
            {
                commonPacketFormat.Data.Add(value[i]);
            }
            //----------------Data

            byte[] dataToWrite = new byte[encapsulation.toBytes().Length + commonPacketFormat.toBytes().Length];
            System.Buffer.BlockCopy(encapsulation.toBytes(), 0, dataToWrite, 0, encapsulation.toBytes().Length);
            System.Buffer.BlockCopy(commonPacketFormat.toBytes(), 0, dataToWrite, encapsulation.toBytes().Length, commonPacketFormat.toBytes().Length);
            encapsulation.toBytes();

            //stream.Write(dataToWrite, 0, dataToWrite.Length);
            //stream.Flush();
            //byte[] data = new Byte[client.ReceiveBufferSize];

            //Int32 bytes = stream.Read(data, 0, data.Length);
            byte[] data;
            Int32 bytes = sendRequest(dataToWrite, out data);

            //--------------------------BEGIN Error?
            if (data[42] != 0)      //Exception codes see "Table B-1.1 CIP General Status Codes"
            {
                throw new CIPException(GeneralStatusCodes.GetStatusCode(data[42]));
            }
            //--------------------------END Error?

            byte[] returnData = new byte[bytes - 44];
            System.Buffer.BlockCopy(data, 44, returnData, 0, bytes - 44);

            return returnData;
        }

        /// <summary>
        /// Get the Encrypted Request Path - See Volume 1 Appendix C (C9)
        /// e.g. for 8 Bit: 20 05 24 02 30 01
        /// for 16 Bit: 21 00 05 00 24 02 30 01
        /// </summary>
        /// <param name="classID">Requested Class ID</param>
        /// <param name="instanceID">Requested Instance ID</param>
        /// <param name="attributeID">Requested Attribute ID - if "0" the attribute will be ignored</param>
        /// <returns>Encrypted Request Path</returns>
        private byte[] GetEPath(int classID, int instanceID, int attributeID)
        {
            //ToDo: Check for Endianess
            int byteCount = 0;
            if (classID < 0xff)
                byteCount = byteCount + 2;
            else
                byteCount = byteCount + 4;

            if (instanceID < 0xff)
                byteCount = byteCount + 2;
            else
                byteCount = byteCount + 4;
            if (attributeID != 0)
                if (attributeID < 0xff)
                    byteCount = byteCount + 2;
                else
                    byteCount = byteCount + 4;

            byte[] returnValue = new byte[byteCount];
            byteCount = 0;
            if (classID < 0xff)
            {
                returnValue[byteCount] = 0x20;
                returnValue[byteCount + 1] = (byte)classID;
                byteCount = byteCount + 2;
            }
            else
            {
                returnValue[byteCount] = 0x21;
                returnValue[byteCount + 1] = 0;                             //Padded Byte
                returnValue[byteCount + 2] = (byte)classID;                 //LSB
                returnValue[byteCount + 3] = (byte)(classID >> 8);            //MSB
                byteCount = byteCount + 4;
            }


            if (instanceID < 0xff)
            {
                returnValue[byteCount] = 0x24;
                returnValue[byteCount + 1] = (byte)instanceID;
                byteCount = byteCount + 2;
            }
            else
            {
                returnValue[byteCount] = 0x25;
                returnValue[byteCount + 1] = 0;                                //Padded Byte
                returnValue[byteCount + 2] = ((byte)instanceID);                 //LSB
                returnValue[byteCount + 3] = (byte)(instanceID >> 8);          //MSB
                byteCount = byteCount + 4;
            }
            if (attributeID != 0)
                if (attributeID < 0xff)
                {
                    returnValue[byteCount] = 0x30;
                    returnValue[byteCount + 1] = (byte)attributeID;
                    byteCount = byteCount + 2;
                }
                else
                {
                    returnValue[byteCount] = 0x31;
                    returnValue[byteCount + 1] = 0;                                 //Padded Byte
                    returnValue[byteCount + 2] = (byte)attributeID;                 //LSB
                    returnValue[byteCount + 3] = (byte)(attributeID >> 8);          //MSB
                    byteCount = byteCount + 4;
                }

            return returnValue;

        }

        /// <summary>
        /// Converts a bytearray (received e.g. via getAttributeSingle) to ushort
        /// </summary>
        /// <param name="byteArray">bytearray to convert</param> 
        public static ushort ToUshort(byte[] byteArray)
        {
            UInt16 returnValue;
            returnValue = (UInt16)(byteArray[1] << 8 | byteArray[0]);
            return returnValue;
        }

        /// <summary>
        /// Converts a bytearray (received e.g. via getAttributeSingle) to uint
        /// </summary>
        /// <param name="byteArray">bytearray to convert</param> 
        public static uint ToUint(byte[] byteArray)
        {
            UInt32 returnValue = ((UInt32)byteArray[3] << 24 | (UInt32)byteArray[2] << 16 | (UInt32)byteArray[1] << 8 | (UInt32)byteArray[0]);
            return returnValue;
        }

        /// <summary>
        /// Returns the "Bool" State of a byte Received via getAttributeSingle
        /// </summary>
        /// <param name="inputByte">byte to convert</param> 
        /// <param name="bitposition">bitposition to convert (First bit = bitposition 0)</param> 
        /// <returns>Converted bool value</returns>
        public static bool ToBool(byte inputByte, int bitposition)
        {

            return (((inputByte >> bitposition) & 0x01) != 0) ? true : false;
        }
    }

    public enum ConnectionType : byte
    {
        Null = 0,
        Multicast = 1,
        Point_to_Point = 2
    }

    public enum Priority : byte
    {
        Low = 0,
        High = 1,
        Scheduled = 2,
        Urgent = 3
    }

    public enum RealTimeFormat : byte
    {
        Modeless = 0,
        ZeroLength = 1,
        Heartbeat = 2,
        Header32Bit = 3


    }
}
