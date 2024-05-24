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
#endregion

public class raC_ETHIPTagLogic : BaseNetLogic
{
    /// <summary>
    /// The raC_ETHIPDevice where the tag belongs to
    /// </summary>
    raC_ETHIPDevice _myDevice;
    /// <summary>
    /// The raC_ETHIPGroup where the tag belongs to
    /// </summary>
    raC_ETHIPTagGroupCore _myGroup;

    /// <summary>
    /// The Value variable of the tag
    /// </summary>
    IUAVariable _ValueVariable;

    /// <summary>
    /// internal indicator if the value change event is subscribed
    /// </summary>
    bool _changeSubscribed;

    bool _pauseEvent;

    bool _simMode;

    public override void Start()
    {
        //Get the Value variable object
         _ValueVariable = Owner.GetVariable("Value");
        if (_ValueVariable == null)
            throw new CoreConfigurationException("Cant use raC_ETHIPTagCore directly. Please use an inherited type.");

        //loop through the owner upwards to get the group and device whete the tag belong to
        IUANode o = Owner;
        while (o!=null)
        {
            if(o is raC_ETHIPTagGroupCore)
            {
                if(_myGroup==null)
                    {
                        _myGroup = (raC_ETHIPTagGroupCore)o;
                    //ToDo: How to deal with this?
                        //_simMode = (_myGroup is raC_ETHIPTagGroupSIM);
                    }
            }
            if(o is raC_ETHIPDevice)
            {
                if(_myDevice==null)
                    _myDevice = (raC_ETHIPDevice)o;
            }
            o=o.Owner;
        }
        //End of group and device loop

        //Error checking
        if(_myGroup==null)
            throw new CoreConfigurationException("The Tag must belong to a tag group.");

        if(_myDevice==null && !_simMode)
            throw new CoreConfigurationException("The Tag must belong to a device.");
    }

    /// <summary>
    /// Leave there, even if there is no reference shown!
    /// This function is called by the group after the group is initialized. 
    /// </summary>
    public void startTag()
    {
        //If configured, read value at start
        if(Owner.GetVariable("ReadOnStart").Value)
            ReadValue();
        //If configured, write value at start
        if (Owner.GetVariable("WriteOnChange/WriteOnStart").Value)
            WriteValue();

        //If configured, subscribe to the value changed event and set the internal flag
        if (Owner.GetVariable("WriteOnChange").Value)
        {
            _ValueVariable.VariableChange+=Value_VariableChange;
            _changeSubscribed=true;
        }
    }

    public override void Stop()
    {
        //Clean up event if flag is set
        if(_changeSubscribed)
        {
            _ValueVariable.VariableChange-=Value_VariableChange;
        }
    }

    /// <summary>
    /// Initialize the read parameter command. The command is send and processed by the group.
    /// </summary>
    [FTOptix.NetLogic.ExportMethod]
    public void ReadValue()
    {
        //Prepare the parameter which need to be passed. Parameters allway are type of Object[]
        raC_ETHIPTagCore p = (raC_ETHIPTagCore)Owner;
        object[] para = new object[] {p.CIPClass,p.CIPInstance,p.CIPAttribute};
        //Return parameter
        object[] res;
        //Send command to group
        try
        {
            _myGroup.GetByType<NetLogicObject>().ExecuteMethod("getSingleParameterCIA", para, out res);
            //ToDO: How to deal with errors?
            //Set the value of the Tag
            _pauseEvent = true;

            setTagValue((byte[])res[0], 0);
        }
        catch (Exception ex)
        {
            setTagValue(new byte[0], 1);
            Log.Warning("Failed to get Parameter '" + Owner.BrowseName + "'." + Environment.NewLine + ex.Message + Environment.NewLine + ex.StackTrace);
        }
    }

    /// <summary>
    /// Monitor Value and write to device
    /// </summary>
    /// <param name="sender"></param>
    /// <param name="e"></param>
    private void Value_VariableChange(object sender, VariableChangeEventArgs e)
    {
        if(!_pauseEvent)
            WriteValue();
    
         _pauseEvent=false;
        
    }
    
    /// <summary>
    /// Write a parameter value to the device using the group as executor.
    /// </summary>
    [FTOptix.NetLogic.ExportMethod]
    public void WriteValue()
    {
        raC_ETHIPTagCore p = (raC_ETHIPTagCore)Owner;
        byte[] data;
        //Get own Value as byte[]
        getTagValue(out data);
        //Prepare the parameter which need to be passed. Parameters allway are type of Object[]
        object[] para = new object[] {p.CIPClass,p.CIPInstance,p.CIPAttribute, data};
        //And send the command to group
        _myGroup.GetByType<NetLogicObject>().ExecuteMethod("setSingleParameterCIA",para);
    }

    /// <summary>
    /// Decodes and set the value of the Tag based of the byte array recieved by CIP MSG
    /// </summary>
    /// <param name="DataStream"></param>
    public void setTagValue(byte[] DataStream, int Quality)
    {
        //ToDo: Add quality parameter!!!
        //Based on the type, use the correspondending decoding
        
        int dataStart = 0;
        raC_ETHIPTagCore p = (raC_ETHIPTagCore)Owner;
        p.CIPQuality = Quality;

        if (Quality == 0)
        {
            Type t = p.GetType();
            if (p is raC_ETHIPTagBOOL)
            {
                raC_ETHIPTagBOOL n = (raC_ETHIPTagBOOL)p;
                //n.ValueVariable.Quality = Quality;
                n.Value = BitConverter.ToBoolean(DataStream, dataStart);
            }
            else if (p is raC_ETHIPTagDINT)
            {
                raC_ETHIPTagDINT n = (raC_ETHIPTagDINT)p;
                n.Value = BitConverter.ToInt32(DataStream, dataStart);
            }
            else if (p is raC_ETHIPTagINT)
            {
                raC_ETHIPTagINT n = (raC_ETHIPTagINT)p;
                n.Value = BitConverter.ToInt16(DataStream, dataStart);
            }
            else if (p is raC_ETHIPTagREAL)
            {
                raC_ETHIPTagREAL n = (raC_ETHIPTagREAL)p;
                n.Value = BitConverter.ToSingle(DataStream, dataStart);
            }
            else if (p is raC_ETHIPTagUDINT)
            {
                raC_ETHIPTagUDINT n = (raC_ETHIPTagUDINT)p;
                n.Value = BitConverter.ToUInt32(DataStream, dataStart);
            }
            else if (p is raC_ETHIPTagUINT)
            {
                raC_ETHIPTagUINT n = (raC_ETHIPTagUINT)p;
                n.Value = BitConverter.ToUInt16(DataStream, dataStart);
            }
            else if (p is raC_ETHIPTagSINT)
            {
                raC_ETHIPTagSINT n = (raC_ETHIPTagSINT)p;
                n.Value = (sbyte)DataStream[dataStart];
            }
            else if (p is raC_ETHIPTagUSINT)
            {
                raC_ETHIPTagUSINT n = (raC_ETHIPTagUSINT)p;
                n.Value = DataStream[dataStart];
            }
            //A custom string with the first 4 bytes are length
            else if (p is raC_ETHIPTagSTRING)
            {
                raC_ETHIPTagSTRING n = (raC_ETHIPTagSTRING)p;
                int len = BitConverter.ToInt32(DataStream, dataStart);
                dataStart += 4;
                n.Value = System.Text.Encoding.ASCII.GetString(DataStream, dataStart + 1, len);
            }
            //A short string with just the first byte are length
            else if (p is raC_ETHIPTagShortSTRING)
            {
                raC_ETHIPTagShortSTRING n = (raC_ETHIPTagShortSTRING)p;
                int len = DataStream[dataStart];
                n.Value = System.Text.Encoding.ASCII.GetString(DataStream, dataStart + 1, len);
            }
            else
            {
                Log.Error(Owner.BrowseName + ": " + p.GetType().ToString() + " not yet implementet!");
            }
        }
    }

    /// <summary>
    /// Gets the value of the Tag as byte array to be send to a CIP device
    /// </summary>
    /// <param name="res"></param>
    public void getTagValue(out byte[] res)
    {
        //Based on the type, use the correspondending convertion
        raC_ETHIPTagCore p = (raC_ETHIPTagCore)Owner;
        Type t = p.GetType();
        if (p is raC_ETHIPTagBOOL) {
            raC_ETHIPTagBOOL n = (raC_ETHIPTagBOOL)p;
                res = new byte[2];
                res[0] = n.Value ? (byte)1:(byte)0;
        }
        else if (p is raC_ETHIPTagDINT) {
            raC_ETHIPTagDINT n = (raC_ETHIPTagDINT)p;
                res = BitConverter.GetBytes(n.Value);
        }
        else if (p is raC_ETHIPTagINT) {
            raC_ETHIPTagINT n = (raC_ETHIPTagINT)p;
                res = BitConverter.GetBytes(n.Value);
        }
        else if (p is raC_ETHIPTagREAL) {
            raC_ETHIPTagREAL n = (raC_ETHIPTagREAL)p;
                res = BitConverter.GetBytes(n.Value);
        }
        else if (p is raC_ETHIPTagUDINT) {
            raC_ETHIPTagUDINT n = (raC_ETHIPTagUDINT)p;
                res = BitConverter.GetBytes(n.Value);
        }
        else if (p is raC_ETHIPTagUINT) {
            raC_ETHIPTagUINT n = (raC_ETHIPTagUINT)p;
                res = BitConverter.GetBytes(n.Value);
        }
        else if (p is raC_ETHIPTagSINT) {
            raC_ETHIPTagSINT n = (raC_ETHIPTagSINT)p;
                res = BitConverter.GetBytes(n.Value);
        }
        else if (p is raC_ETHIPTagUSINT) {
            raC_ETHIPTagUSINT n = (raC_ETHIPTagUSINT)p;
                res = BitConverter.GetBytes(n.Value);
        }
        //A custom string with the first 4 bytes are length
        else if (p is raC_ETHIPTagSTRING) {
            raC_ETHIPTagSTRING n = (raC_ETHIPTagSTRING)p;
            Int32 len = System.Text.Encoding.ASCII.GetByteCount(n.Value);
            res = new byte[len+4];
            Array.Copy(BitConverter.GetBytes(len),res,4);
            System.Text.Encoding.ASCII.GetBytes(n.Value).CopyTo(res,4); 
        }
        //A short string with just the byte is length
        else if (p is raC_ETHIPTagShortSTRING)
        {
            raC_ETHIPTagShortSTRING n = (raC_ETHIPTagShortSTRING)p;
            res = new byte[System.Text.Encoding.ASCII.GetByteCount(n.Value) + 1];
            res[0] = (byte)n.Value.Length;
            System.Text.Encoding.ASCII.GetBytes(n.Value).CopyTo(res, 1);
        }
        else {
            Log.Error(Owner.BrowseName + ": " + p.GetType().ToString() + " not yet implementet!");
            res = null;
        }
    }
}
