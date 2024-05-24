#region Using directives
using System;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.HMIProject;
using FTOptix.Retentivity;
using FTOptix.UI;
using FTOptix.NativeUI;
using FTOptix.NetLogic;
using FTOptix.CoreBase;
using FTOptix.Core;
using FTOptix.Store;
#endregion

public class CLXTaskLoadParserLogic : BaseNetLogic
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
        int numTask = BitConverter.ToInt16(raw, 0) * 4;
        Int16 unused = 0, motion = 0, system = 0, fault = 0, user = 0;

        for (int i = 4; i < numTask; i += 4)
        {
            Int16 taskType = BitConverter.ToInt16(raw, i);
            switch (taskType)
            {
                case -1:
                    unused = BitConverter.ToInt16(raw, i + 2);
                    break;
                case -2:
                    motion = BitConverter.ToInt16(raw, i + 2);
                    break;
                case -3:
                    fault = BitConverter.ToInt16(raw, i + 2);
                    break;
                case -4:
                    system += BitConverter.ToInt16(raw, i + 2);
                    break;
                case > 0:
                    user += BitConverter.ToInt16(raw, i + 2);
                    break;

                default:
                    break;
            }
        }

        Owner.GetVariable("CtrlUse/Unused").Value = unused;
        Owner.GetVariable("CtrlUse/Motion").Value = motion;
        Owner.GetVariable("CtrlUse/Fault").Value = fault;
        Owner.GetVariable("CtrlUse/System").Value = system;
        Owner.GetVariable("CtrlUse/User").Value = user;
    }
}
