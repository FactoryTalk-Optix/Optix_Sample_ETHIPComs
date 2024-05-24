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
using System.Timers;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Diagnostics;
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
using System.Net;
#endregion

public class raC_ETHIPGroupLogic : BaseNetLogic
{
    /// <summary>
    /// EthernetIP Connection
    /// </summary>
    //ToDo: remove client here and do all comms via device object and executeMethod  
    //private ETHIPClient _ethipClient;

    //Tag Update timer.
    private System.Timers.Timer _tmrUpdate;

    /// <summary>
    /// The ETHIP Device where the group belongs to
    /// </summary>
    private raC_ETHIPDevice _myDevice;

    /// <summary>
    /// The ETHIP Group
    /// </summary>
    private raC_ETHIPTagGroup  _myGroup;

    private IUAVariable _RPIVariable;

    private IUAVariable _logStatsVariable;
    private IUAVariable _cyclicReadVariable;

    /// <summary>
    /// List of all Tag in this group. Filled at start() recursevely to allow sub-folder and objects
    /// </summary>
    private List<raC_ETHIPTagCore> _knownTags;
    private List<raC_ETHIPCustomMessageCore> _knownServices;
    
    public override void Start()
    {
         _myGroup = (raC_ETHIPTagGroup)Owner;

        //loop through the owner upwards to get the device whete the tag belong to
        IUANode o = _myGroup.Owner;

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
        if (_myDevice==null)
            throw new CoreConfigurationException("The Tag Group must belong to a device.");
        
        if(_myGroup.Sts_Running)
        {
            Log.Error (Owner.BrowseName + ": " + "Allready running");
            return;
        }

        _RPIVariable=_myGroup.GetVariable("CyclicReadEnabled/RPI");
        _logStatsVariable = _myDevice.GetVariable("LogStatistics");
        _cyclicReadVariable = _myGroup.GetVariable("CyclicReadEnabled");

//ToDo: Should this be in seperate function to allow runtime updates on config?
//Be aware that this is also used in manual read/write operations
        _knownTags = new List<raC_ETHIPTagCore>();
        _knownServices = new List<raC_ETHIPCustomMessageCore>();

        //Add all Tags, even if in subfolder or objects to allow better structuring
        //ToDo: Should we enforce the "Tags" subobject???
        //addKnowTags(_myGroup.GetObject("Tags"));
        addKnowTags(_myGroup);
    }

    /// <summary>
    /// Add Tags to the internal lists of all tags inside the root recursive.
    /// </summary>
    /// <param name="root">The object to look in</param>
    private void addKnowTags(IUANode root)
    {
        foreach (var item in root.Children)
        {
            //Only process type TagCommon based objects
            if(item is raC_ETHIPTagCore)
            {
                //Add tag to lists
                _knownTags.Add((raC_ETHIPTagCore)item);
                //Start the Tag to allow "Read/Write at start" functionality
                ((raC_ETHIPTagCore)item).raC_ETHIPTagLogic.ExecuteMethod("startTag");
            }else if (item is raC_ETHIPCustomMessageCore)
            {
                //Add tag to lists
                _knownServices.Add((raC_ETHIPCustomMessageCore)item);
            }
            else //If no Tag, next level
                addKnowTags(item);
        }
    }

    public override void Stop()
    {
        StopGroup();
    }

    /// <summary>
    /// Starts the cyclic message processing
    /// </summary>
    ///[FTOptix.NetLogic.ExportMethod]
    public void StartGroup()
    {
        try
        {
            //Initialice the ETHIP Client
            //_ethipClient = new ETHIPClient();
            //_ethipClient.IPAddress=_myDevice.Path;
            
            //Set timer props. NOt start the timer here, its done later.
            _tmrUpdate = new System.Timers.Timer();
            _tmrUpdate.Interval = _RPIVariable.Value;
            _tmrUpdate.Elapsed += processCyclic;
            
            //Do an initial async read to not block the HMI.
            Task.Run(() => ReadAll(true));
            
            //Set status
            _myGroup.Sts_Running = true;
            Log.Debug(Owner.BrowseName + "Started");
         }
        catch(Exception ex)
        {
            Log.Error (Owner.BrowseName + ": " + ex.Message + Environment.NewLine + ex.StackTrace.ToString());
        }
    }

    /// <summary>
    /// Stops the cyclic processing of the group
    /// </summary>
    //[FTOptix.NetLogic.ExportMethod]
    public void StopGroup()
    {
       try
        {
            Log.Debug("Stopping");

            //Stop timer
            if(_tmrUpdate!=null)
            {
                _tmrUpdate.Stop();
                _tmrUpdate.Elapsed -= processCyclic;
            }
            //Unregister CIP session 
            //if(_ethipClient!=null)
            // {
            //     try
            //     {
            //         _ethipClient.UnRegisterSession();
            //         _ethipClient.Client.Close();
            //     }
            //     catch
            //     {
                 
            //     }
            // }
            
            //Set the status
            _myGroup.Sts_Running = false;
            Log.Debug("Stopped");
        }
        catch(Exception ex)
        {
            Log.Error (Owner.BrowseName + ": " + ex.Message + Environment.NewLine + ex.StackTrace.ToString());
        }
    }

    /// <summary>
    /// Do a one time async read of all tags in the group
    /// </summary>
    [FTOptix.NetLogic.ExportMethod]
    public void ReadOnceAsync()
    {
        Task.Run(() => ReadOnce());
    }

    /// <summary>
    /// Do a sync read of all tags in the group
    /// </summary>
    [FTOptix.NetLogic.ExportMethod]
    public void ReadOnce()
    {
        //If we are not connected, no cyclic update started, create new connection
        // bool cleanup=false;
        // if(_ethipClient==null)
        // {
        //     _ethipClient = new ETHIPClient();
        //     _ethipClient.IPAddress=_myDevice.Path;
        //     cleanup=true;
        // }
        //read all tags
        ReadAll(false);

        //If we created a new connection, cleanup
        // if(cleanup)
        // {
        //     _ethipClient.UnRegisterSession();
        //     _ethipClient=null;
        // }
    }

    /// <summary>
    /// Reads all tags belonging to this group and updates the values of the tags
    /// </summary>
    /// <param name="startTimer">Start the timer after processing</param>
    private void ReadAll(bool startTimer)
    {       
        try
        {
            //TODO: can we use multi service request? Is this supported on all devices?

            //Process every tag one by one
            foreach (var item in _knownTags)
            {
                if(((raC_ETHIPTagCore)item).CIPClass > 0)
                    getSingleParameter((raC_ETHIPTagCore)item);
            }
            foreach (var item in _knownServices)
            {
                item.GetByType<NetLogicObject>().ExecuteMethod("executeService");
            }

        }
        catch (System.Exception ex)
        {
            Log.Error (Owner.BrowseName + ": " + ex.Message + Environment.NewLine + ex.StackTrace.ToString());
        }

        //Start timer if needed
        if(startTimer)
            _tmrUpdate.Start();
    }

    /// <summary>
    /// Do a one time async write of all tags in the group
    /// </summary>
    [FTOptix.NetLogic.ExportMethod]
    public void WriteOnceAsync()
    {
        Task.Run(() => WriteOnce());
    }

    /// <summary>
    /// Do a one time sync write of all tags in the group
    /// </summary>
    [FTOptix.NetLogic.ExportMethod]
    public void WriteOnce()
    {
        //If we are not connected, no cyclic update started, create new connection
        // bool cleanup =false;
        // if(_ethipClient==null)
        // {
        //     _ethipClient = new ETHIPClient();
        //     _ethipClient.IPAddress=_myDevice.Path;
        //     cleanup=true;
        // }

        //Writes all tag values of the group
        WriteAll();

        //Close connection, if we opened it here
        // if(cleanup)
        // {
        //     _ethipClient.UnRegisterSession();
        //     _ethipClient=null;
        // }
    }

    /// <summary>
    /// Writes the values of all tag of this group
    /// </summary>
    private void WriteAll()
    {
        try
        {
            //Register CIP session
            //_ethipClient.RegisterSession();

            //Process each tag one after another
            foreach (var item in _knownTags)
            {
                if(((raC_ETHIPTagCore)item).CIPClass > 0)
                    setSingleParameter((raC_ETHIPTagCore)item);
            }
                
        }
        catch (System.Exception ex)
        {
            Log.Error (Owner.BrowseName + ": " + ex.Message + Environment.NewLine + ex.StackTrace.ToString());
        }
    }

    //The update timer is done
    private void processCyclic(object source, ElapsedEventArgs e)
    {
        bool restart=false;

         _tmrUpdate.Stop();
        
        Stopwatch sw = Stopwatch.StartNew();
        //Should we do a cyclic read?
        if(_cyclicReadVariable.Value)
            ReadAll(false); restart=true;

        if(_logStatsVariable.Value)
            Log.Info("Processed group '" + _myGroup.BrowseName + "' of '" + _myDevice.BrowseName + "' in [ms]: " + sw.ElapsedMilliseconds.ToString());
        //Restart timer?
        if(restart)
        {
            _tmrUpdate.Interval = _RPIVariable.Value - sw.ElapsedMilliseconds > 1 ? _RPIVariable.Value - sw.ElapsedMilliseconds:1;
            _tmrUpdate.Start();
        }
    }

    /// <summary>
    /// Reads a single parameter from the device and set the tag value.
    /// </summary>
    /// <param name="p">The Tag to process.</param>
    public void getSingleParameter(raC_ETHIPTagCore p)
    {
        byte[] res;
        //Call the underlying function
        try
        { 
            getSingleParameterCIA(p.CIPClass,p.CIPInstance,p.CIPAttribute, out res);
            //send the command for updating the value to the tag.

            ((raC_ETHIPTagCore)p).raC_ETHIPTagLogic.ExecuteMethod("setTagValue",new object[] {res, 0});
        }
        catch (Exception ex)
        {
            ((raC_ETHIPTagCore)p).raC_ETHIPTagLogic.ExecuteMethod("setTagValue", new object[] { new byte[0], 1 });
            Log.Error("Failed to get Parameter '" + p.BrowseName + "'. " + Environment.NewLine + ex.Message + Environment.NewLine + ex.StackTrace);
        }
    }

    /// <summary>
    ///  Get the tag value and write a single parameter to the device.
    /// </summary>
    /// <param name="p">The Tag to process.</param>
    public void setSingleParameter(raC_ETHIPTagCore p)
    {
        object[] res;
        //Send the command to the tag to get the actual value as byte[]
        ((raC_ETHIPTagCore)p).raC_ETHIPTagLogic.ExecuteMethod("getTagValue",new object[0],out res);
        //Call the underlying function
        setSingleParameterCIA(p.CIPClass,p.CIPInstance,p.CIPAttribute, (byte[])res[0]);
    }

    /// <summary>
    /// Send a command to the device to get a single parameter value.
    /// </summary>
    /// <param name="Class">CIP Class</param>
    /// <param name="Instance">CIP Instance</param>
    /// <param name="Attribute">CIP Attribute</param>
    /// <param name="res">Result as byte array</param>
    //[FTOptix.NetLogic.ExportMethod]
    public void getSingleParameterCIA(int Class, int Instance, int Attribute, out byte[] res)
    {
        // bool cleanup=false;
        // if(_ethipClient==null)
        // {
        //     _ethipClient = new ETHIPClient();
        //     _ethipClient.IPAddress=_myDevice.Path;
        //     cleanup=true;
        // }

        //res = _ethipClient.GetAttributeSingle(Class,Instance,Attribute);
        object[] paraSend = new object[] {Class,Instance,Attribute};
        object[] paraRecieved;
        _myDevice.raC_ETHIPDeviceLogic.ExecuteMethod("getSingleParameterCIA",paraSend,out paraRecieved);
        res = (byte[])paraRecieved[0];   
        
        // if(cleanup)
        // {
        //     _ethipClient.UnRegisterSession();
        //     _ethipClient=null;
        // }
    }

    /// <summary>
    /// Send a command to the device to set a single parameter value.
    /// </summary>
    /// <param name="Class">CIP Class</param>
    /// <param name="Instance">CIP Instance</param>
    /// <param name="Attribute">CIP Attribute</param>
    /// <param name="data">value as byte array</param>
    //[FTOptix.NetLogic.ExportMethod]
    public void setSingleParameterCIA(int Class, int Instance, int Attribute, byte[] data)
    {
        // bool cleanup=false;
        // if(_ethipClient==null)
        // {
        //     _ethipClient = new ETHIPClient();
        //     _ethipClient.IPAddress=_myDevice.Path;
        //     cleanup=true;
        // }

        //_ethipClient.SetAttributeSingle(Class,Instance,Attribute,data);    
        object[] paraSend = new object[] {Class,Instance,Attribute,data};
        _myDevice.raC_ETHIPDeviceLogic.ExecuteMethod("setSingleParameterCIA",paraSend);
        
        // if(cleanup)
        // {
        //     _ethipClient.UnRegisterSession();
        //     _ethipClient=null;
        // }
    }
    
}
