#region Using directives
using System;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.SQLiteStore;
using FTOptix.HMIProject;
using FTOptix.NetLogic;
using FTOptix.UI;
using FTOptix.NativeUI;
using FTOptix.Store;
using FTOptix.Retentivity;
using FTOptix.CoreBase;
using FTOptix.Core;
#endregion

public class CLXCoreLoadParserLogic : BaseNetLogic
{
    public override void Start()
    {
        // Insert code to be executed when the user-defined logic is started
    }

    public override void Stop()
    {
        // Insert code to be executed when the user-defined logic is stopped
    }

    [ExportMethod]
    public void DecodeData()
    {
        byte[] raw = Owner.GetVariable("DataIn").Value;
        int numCores = raw[0];
        
        for (int i = 0; i < 4; i++)
        {
            Owner.GetVariable("Core" + i.ToString() + "/Type").Value = i < numCores ? raw[4 + (i*2)] : 0;
            Owner.GetVariable("Core" + i.ToString() + "/Percentage").Value = i < numCores ? raw[4 + 1 + +(i * 2)] : 0;
        }
    }
}
