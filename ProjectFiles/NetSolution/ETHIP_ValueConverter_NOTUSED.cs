using System;
using UAManagedCore;
using FTOptix.Store;
using FTOptix.ODBCStore;
using FTOptix.SQLiteStore;
using FTOptix.AuditSigning;
using FTOptix.DataLogger;
using FTOptix.System;
using FTOptix.OPCUAServer;
using FTOptix.EventLogger;
using FTOptix.RAEtherNetIP;
using FTOptix.CommunicationDriver;

namespace RATC.ETHIP
{
    static class raC_ETHIPValueConverter
    {
        public static T ByteToValue<T>(byte[] data)
        {
        Type t = typeof(T);
        if (t == typeof(bool)) {
            return(T) Convert.ChangeType(BitConverter.ToInt16(data, 0) != 0,t);
        }
        else if (t == typeof(Int32)) {
            return(T) Convert.ChangeType(BitConverter.ToInt32(data, 0),t);
        }
        else if (t == typeof(Int16)) {
            return(T) Convert.ChangeType(BitConverter.ToInt16(data, 0),t); 
        }
        else if (t == typeof(Single)) {
            return(T) Convert.ChangeType(BitConverter.ToSingle(data, 0),t); 
        }
        else if (t == typeof(UInt32)) {
            return(T) Convert.ChangeType(BitConverter.ToUInt32(data, 0),t);
        }
        else if (t == typeof(UInt16)) {
            return(T) Convert.ChangeType(BitConverter.ToUInt16(data, 0),t);    
        }
        else if (t == typeof(sbyte)) {
            return(T) Convert.ChangeType(data[0],t);
        }
         else if (t == typeof(byte)) {
            return(T) Convert.ChangeType(data[0],t);
        }
        else if (t == typeof(string)) {
            int len = data[0];
            return(T) Convert.ChangeType(System.Text.Encoding.ASCII.GetString(data, 0+1, len),t);
        }
        else {
            throw new NotSupportedException(t.GetType().ToString() + " not yet implementet!");
        }
        }
    }

}
