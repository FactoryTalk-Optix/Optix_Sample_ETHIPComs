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
using System.IO;
using System.Collections.Generic;
using System.Text.RegularExpressions;
using System.Text;
using RATC.ETHIP;
using FTOptix.Store;
#endregion

public class EDSFileImporter_DesignTime : BaseNetLogic
{
    [ExportMethod]
    public void importAsDeviceType()
    {
        try
        {
            Uri EDSPath = new Uri(LogicObject.GetVariable("EDSFilePath").Value);
            IUANode dest = LogicObject.GetAlias("Destination");
            string catalog = "";
            bool paramsfound = false;
            List<IUAObject> para = new List<IUAObject>();

            StreamReader sr = new StreamReader(EDSPath.LocalPath);
            //Read the first line of text
            string line = sr.ReadLine();
            //Continue to read until you reach end of file
            while (line != null)
            {
                if (line.Contains(@"[Device]"))
                {
                    while (line != null)
                    {
                        if (line.Contains(@"Catalog"))
                        {
                            string[] tmp = line.Split("=");
                            catalog = tmp[1].Trim().Replace(@"""", "").Replace(";", "");
                            break;
                        }
                        if (sr.EndOfStream)
                            throw new CoreConfigurationException("Catalog not found.");

                        line = sr.ReadLine();
                    }
                    //break;
                }

                if (line.Contains(@"[Params]"))
                {
                    paramsfound = true;
                    line = sr.ReadLine();
                    while (line != null)
                    {
                        if (line.Trim().StartsWith("Param"))
                        {
                            StringBuilder p = new StringBuilder();
                            string[] tmp = line.Split("=");
                            string paraname = tmp[0].Trim();
                            p.Append(tmp[1].Trim());
                            line = sr.ReadLine();
                            p.Append(Regex.Replace(line, @"\$.*", "").Trim());
                            line = sr.ReadLine();
                            p.Append(Regex.Replace(line, @"\$.*", "").Trim());
                            line = sr.ReadLine();
                            p.Append(Regex.Replace(line, @"\$.*", "").Trim());
                            line = sr.ReadLine();
                            p.Append(Regex.Replace(line, @"\$.*", "").Trim());
                            line = sr.ReadLine();
                            p.Append(Regex.Replace(line, @"\$.*", "").Trim());
                            line = sr.ReadLine();
                            p.Append(Regex.Replace(line, @"\$.*", "").Trim());
                            line = sr.ReadLine();
                            p.Append(Regex.Replace(line, @"\$.*", "").Trim());
                            line = sr.ReadLine();
                            p.Append(Regex.Replace(line, @"\$.*", "").Trim());
                            line = sr.ReadLine();
                            p.Append(Regex.Replace(line, @"\$.*", "").Trim());
                            line = sr.ReadLine();
                            p.Append(Regex.Replace(line, @"\$.*", "").Trim());
                            line = sr.ReadLine();
                            if (Regex.Replace(line, @"\$.*", "").Trim() == ";")
                            {
                                p.Append("0");
                            }
                            else
                                p.Append(Regex.Replace(line, @"\$.*", "").Replace(";", "").Trim());

                            string[] cols = p.ToString().Split(",");
                            //Log.Info(cols[4] + " - " + cols[6]);
                            string ciptypes = getDataType(int.Parse(cols[4].Replace("0x", ""), System.Globalization.NumberStyles.HexNumber));

                            if (ciptypes != null)
                            {
                                UAObjectType tt = null;
                                foreach (var item in Owner.GetObject("raC_ETHIPTagTypes").Children)
                                {
                                    if (item.BrowseName == ciptypes)
                                    {
                                        tt = (UAObjectType)(item);
                                        break;
                                    }
                                }
                                if (tt == null)
                                    Log.Warning("Tag Type not found: " + cols[4] + " - " + ciptypes);
                                else
                                {
                                    if (cols[2].Replace(@"""", "").Trim() != "")
                                    {
                                        uint cls, ins, att;
                                        getCIPPathParts(cols[2].Replace(@"""", "").Trim(), out cls, out ins, out att);
                                        IUAObject tag = InformationModel.MakeObject(cols[6].Replace(@"""", "").Trim(), tt.NodeId);
                                        tag.GetVariable("ParameterName").Value = tag.BrowseName;
                                        tag.GetVariable("Unit").Value = cols[7].Replace(@"""", "").Trim();
                                        tag.GetVariable("CIPClass").Value = cls;
                                        tag.GetVariable("CIPInstance").Value = ins;
                                        tag.GetVariable("CIPAttribute").Value = att;
                                        para.Add(tag);
                                    }
                                }
                            }else
                                Log.Warning("Datatype not (yet) supported: " + cols[4] + " for parameter: " + cols[6]);
                        }
                        if (line.Trim().StartsWith("["))
                            break;
                        line = sr.ReadLine();
                    }
                }

                //if (sr.EndOfStream)
                //    throw new CoreConfigurationException("Device not found.");
                //Read the next line

                if (paramsfound)
                    break;

                line = sr.ReadLine();
            }

            //close the file
            sr.Close();

            UAObjectType dt = null;
            foreach (var item in Owner.Children)
            {
                if (item.BrowseName == "raC_ETHIPDevice")
                {
                    dt = (UAObjectType)(item);
                    break;
                }
            }

            UAObjectType gt = null;
            foreach (var item in Owner.Children)
            {
                if (item.BrowseName == "raC_ETHIPTagGroupInhibited")
                {
                    gt = (UAObjectType)(item);
                    break;
                }
            }

            if (dt == null)
                throw new CoreConfigurationException("raC_ETHIPDevice Type not found.");

            if (gt == null)
                throw new CoreConfigurationException("raC_ETHIPTagGroupInhibited Type not found.");

            IUAObject existing = dest.GetObject(catalog);
            if (existing == null)
            {
                IUAObject device = InformationModel.MakeObject(catalog, dt.NodeId);
                IUAObject group = InformationModel.MakeObject("Unscheduled", gt.NodeId);

                foreach (var item in para)
                {
                    group.Add(item);
                }

                device.Add(group);
                dest.Add(device);

                Log.Info("Created Device '" + catalog + "'");
            }
            else
            {
                Log.Info("Device '" + catalog + "' allready found, skipping.");
            }

        }
        catch (System.Exception ex)
        {
            Log.Error(ex.Message + Environment.NewLine + ex.StackTrace);
        }
    }

    private string getDataType(int typeCode)
    {
        string res = null;

        switch (typeCode)
        {

            case 0xC1: // Logical Boolean with values True and False
                res = "raC_ETHIPTagBOOL";
                break;
            case 0xC2: // Signed 8-bit integer
                res = "raC_ETHIPTagSINT";
                break;
            case 0xC3:// Signed 16-bit integer
                res = "raC_ETHIPTagINT";
                break;
            case 0xC4:// Signed 32-bit integer
                res = "raC_ETHIPTagDINT";
                break;
            //case 0xC5:// Signed 64-bit integer
            //    res = "raC_ETHIPTagLINT";
            //    break;
            case 0xC6:// Unsigned 8-bit integer
                res = "raC_ETHIPTagUSINT";
                break;
            case 0xC7:// Unsigned 16-bit integer
                res = "raC_ETHIPTagUINT";
                break;
            case 0xC8:// Unsigned 32-bit integer
                res = "raC_ETHIPTagUDINT";
                break;
            //case 0xC9:// Unsigned 64-bit integer
            //    res = "raC_ETHIPTagULINT";
            //    break;
            case 0xCA:// 32-bit floating point value
                res = "raC_ETHIPTagREAL";
                break;
            //case 0xCB:// 64-bit floating point value
            //    res = "raC_ETHIPTagLREAL";
            //    break;
            case 0xD0:// bit string:8-bits
                res = "raC_ETHIPTagSTRING";
                break;
            case 0xD1:// bit string:8-bits
                res = "raC_ETHIPTagSINT";
                break;
            case 0xD2:// bit string:16-bits
                res = "raC_ETHIPTagINT";
                break;
            case 0xD3:// bit string:32-bits
                res = "raC_ETHIPTagDINT";
                break;
            case 0xD4:// bit string:64-bits
            //    res = "raC_ETHIPTagDINT";
            //    break;
            case 0xDA:// Short String
                res = "raC_ETHIPTagShortSTRING";
                break;
            case 0x0FCE: // STRING82_TYPE
                res = "raC_ETHIPTagSTRING";
                break;
            default:
                break;
        }

        return res;
    }

    private void getCIPPathParts(string path,out uint CIPClass, out uint CIPInstance, out uint CIPAttribute)
    {
        string[] parts = path.Split(" ");
        byte[] partbytes = new byte[parts.Length];
        int index = 0;

        for (int i = 0; i < parts.Length; i++)
        {
            partbytes[i] = byte.Parse(parts[i], System.Globalization.NumberStyles.HexNumber);
        }

        //Class
        if (partbytes[index] == 0x20)
        {
            CIPClass = partbytes[index + 1];
            index += 2;
        }else
        {
            CIPClass = BitConverter.ToUInt16( partbytes,index + 1);
            index += 3;
        }

        //instance
        if (partbytes[index] == 0x24)
        {
            CIPInstance = partbytes[index + 1];
            index += 2;
        }
        else
        {
            CIPInstance = BitConverter.ToUInt16(partbytes, index + 1);
            index += 3;
        }

        //Attribute
        if (partbytes[index] == 0x30)
        {
            CIPAttribute = partbytes[index + 1];
            index += 2;
        }
        else
        {
            CIPAttribute = BitConverter.ToUInt16(partbytes, index + 1);
            index += 3;
        }
    }
}
