#region Using directives
using System;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.SQLiteStore;
using FTOptix.HMIProject;
using FTOptix.NetLogic;
using FTOptix.NativeUI;
using FTOptix.UI;
using FTOptix.Store;
using FTOptix.Retentivity;
using FTOptix.AuditSigning;
using FTOptix.CoreBase;
using FTOptix.Core;
using FTOptix.RAEtherNetIP;
using FTOptix.CommunicationDriver;
using FTOptix.DataLogger;
using FTOptix.OPCUAServer;
using System.Reflection;
#endregion

public class raC_ETHIPCustomMessageLogic : BaseNetLogic
{
    private raC_ETHIPDevice _myDevice;

    private IUAVariable DataInVar;
    private IUAVariable DataOutVar;
    private IUAVariable CIPServiceVar;
    private IUAVariable CIPClassVar;
    private IUAVariable CIPInstanceVar;
    private IUAVariable CIPAttributeVar;
    
    public override void Start()
    {
         //loop through the owner upwards to get the device whete the tag belong to
        IUANode o = Owner;

        while (o!=null)
        {
            if(o is raC_ETHIPDevice)
            {
                _myDevice = (raC_ETHIPDevice)o;
                break;
            }
            o=o.Owner;
        }
        //End of device loop

        //Error checking
        //ToDo: Add item name in error -> for all object types
        if (_myDevice==null)
            throw new CoreConfigurationException("The Tag Group must belong to a device.");

        CIPServiceVar = Owner.GetVariable("CIPService");
        CIPClassVar = Owner.GetVariable("CIPClass");
        CIPInstanceVar = Owner.GetVariable("CIPInstance");
        CIPAttributeVar = Owner.GetVariable("CIPAttribute");
        DataInVar = Owner.GetVariable("DataIn");
        DataOutVar = Owner.GetVariable("DataOut");
    }

    public override void Stop()
    {
        // Insert code to be executed when the user-defined logic is stopped
    }

    [FTOptix.NetLogic.ExportMethod]
    public void executeService()
    {
        byte[] data = DataOutVar.Value;
        object[] paraSend = new object[] { (int)CIPServiceVar.Value, (int)CIPClassVar.Value, (int)CIPInstanceVar.Value, (int)CIPAttributeVar.Value, data};
        object[] paraRecieved;
        _myDevice.raC_ETHIPDeviceLogic.ExecuteMethod("executeCustomService",paraSend,out paraRecieved);

        //int l = ((byte[])paraRecieved[0]).Length;

        //int i = 0;
        //foreach (var item in (byte[])paraRecieved[0])
        //{
        //    if (i >= DataInVar.ArrayDimensions[0])
        //        continue;

        //    DataInVar.SetValue(item, new uint[] { (uint)i });
        //    i++;
        //}
        uint dim = DataInVar.ArrayDimensions[0];
        byte[] val = new byte[dim];
        ((byte[])paraRecieved[0]).CopyTo(val, 0);
        DataInVar.SetValue(val);
    }
}
