using System;
using System.Collections.Generic;
using System.Data.Odbc;
using System.Globalization;
using System.IO;
using System.Text;
using System.Threading;
using System.Windows;
using System.Windows.Threading;
using Raw2CDF.Properties;
using Microsoft.Research.Science.Data;
using Microsoft.Research.Science.Data.Factory;
using Microsoft.Research.Science.Data.Imperative;
using MSFileReaderLib;
using NLog;


namespace Raw2CDF
{
    /// <summary>
    /// Interaction logic for MainWindow.xaml
    /// </summary>
    public partial class MainWindow : Window
    {
        
        Logger logger;
        
        public MainWindow()
        {
            InitializeComponent();
            logger = LogManager.GetLogger("Main");
            statusLabel.Content = "Started";
        }
        private void ProcessRun(string folder_str)
        {
            if (string.IsNullOrWhiteSpace(folder_str))
            {
                MessageBox.Show("Path to folder should be specified!", "Folder path", MessageBoxButton.OK,
                    MessageBoxImage.Error);
                return;
            }
            if (!Directory.Exists(folder_str))
                Directory.CreateDirectory(folder_str);

            var connStr = Settings.Default.ConnectionString;
            if (string.IsNullOrWhiteSpace(connStr))
                connStr = $"DSN=MonetDB;Host=scalpelhost;Port=50000;Database=msinvent;Uid=user;Pwd=password;";
            var resultSpectrum = new List<object[]>();
            var querySpectrum = $@"select distinct rtrim(t.label) || '-' || sm.label || '-' || s.id, 
                                    'Extraction', 
                                    'Melted samples in ' || sol.name,
                                    'none',
                                    'N/A',
                                    'Chromatography',
                                    'Mass spectrometry',
                                    m.name,
                                    r.min || '-' || r.max,
                                    dev.name,
                                    'MS:electrospray ionization',
                                    sol.name,
                                    res.name,
                                    s.id,
                                    p.sex,
				                    p.yob,
				                    d.name,
                                    s.filename
                            from tissue t
                                inner join spectrum s on t.id = s.sampletumorid
	                            INNER join patient p on p.id = t.patientid
	                            inner join smpl sm on sm.id = s.sampleid
	                            inner join device dev on dev.id = s.device
	                            inner join mode m on m.id = s.mode
	                            inner join mzrange r on r.id = s.mzrange
	                            inner join solvent sol on sol.id = s.solvent
	                            inner join resolution res on res.id = s.resolutionid
                                inner join diagnosis d on d.id = t.diagnosis
                         where s.ionsource < 5 and not t.label like ('%unspecified%');";
            using (OdbcConnection con = new OdbcConnection(connStr))
            {
                con.Open();
                using (OdbcCommand com = new OdbcCommand(querySpectrum, con))
                {
                    using (OdbcDataReader reader = com.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            var result = new object[17];
                            var obj = reader.GetValues(result);
                            if (result[9].Equals("LTQ_Leninsky"))
                            {
                                result[9] = "MS:LTQ FT Ultra";
                                if (result[12].Equals("FT"))
                                {
                                    result[11] = "MS:fourier transform ion cyclotron resonance mass spectrometer";
                                }
                                else
                                {
                                    result[11] = "MS:linear ion trap";
                                }
                            }
                            else
                            {
                                result[9] = "MS:LTQ Orbitrap XL ETD";
                                if (result[12].Equals("FT"))
                                {
                                    result[11] = "MS:orbitrap";
                                }
                                else
                                {
                                    result[11] = "MS:linear ion trap";
                                }
                            }
                            resultSpectrum.Add(result);
                        }
                        
                    }
                    foreach (object[] objects in resultSpectrum)
                    {
                        var spectrum = new MsSpectrum {Id = (int)objects[13], RawFilePath = (string)objects[17]};
                        var loggerStr = $"Processing spectrum, id = {spectrum.Id}";
                        logger.Info($"Processing spectrum, id = {spectrum.Id}");
                        Dispatcher.BeginInvoke(new Action(() =>
                        {
                            statusLabel.Content = loggerStr;
                        }), DispatcherPriority.Background);

                        Thread piThread = new Thread(() =>
                        {
                            try
                            {
                                SaveDataToCdf(spectrum, folder_str);
                            }
                            finally
                            {
                                spectrum = null;
                            }
                        })
                        {
                            IsBackground = true
                        };
                        piThread.Start();
                    }
                }
            }
            var fstr = new StreamWriter(Path.Combine(folder_str, $"list.txt"));
            foreach (var spectrum in resultSpectrum)
            {
                var line = new StringBuilder(String.Join(";", spectrum));
                line.Append($";{Path.Combine(folder_str, $"{spectrum[13]}.cdf")}");
                fstr.WriteLine(line.ToString());
            }
            fstr.Flush();
            fstr.Close();
        }

        private void OnClick_RunButton(object sender, RoutedEventArgs e)
        {
            var text = targetFolderTextBox.Text;
            Thread piThread = new Thread(() => ProcessRun(text))
            {
                IsBackground = true
            };
            piThread.Start();
        }

        private void SaveDataToCdf(MsSpectrum spec, string fpath)
        {
            DataSetFactory.SearchFolder("Microsoft.Research.Science.Data.NetCDF4.dll");
            DataSetFactory.SearchFolder("Microsoft.Research.Science.Data.dll");
            DataSetFactory.SearchFolder("Microsoft.Research.Science.Data.Imperative.dll");

            int nControllerType = 0; // 0 == mass spec device
            int nContorllerNumber = 1; // first MS device
            int totalNumScans = 0;      // Number of scans
            int firstScanNumber = 0, lastScanNumber = -1;   // Number of first and last scan
            string scanFilter = null;                     // Scan filter line
            int numDataPoints = -1; // points in both the m/z and intensity arrays
            double retentionTimeInMinutes = -1;
            double minObservedMZ = -1;
            double maxObservedMZ = -1;
            double totalIonCurrent = -1;
            double basePeakMZ = -1;
            double basePeakIntensity = -1;
            int channel = 0; // unused
            int uniformTime = 0; // unused
            double frequency = 0; // unused
            
            int arraySize = -1;
            object rawData = null; // rawData wil come as Double[,]
            object peakFlags = null;
            string szFilter = null;        // No filter
            int intensityCutoffType = 1;        // No cutoff
            int intensityCutoffValue = 0;    // No cutoff
            int maxNumberOfPeaks = 0;        // 0 : return all data peaks
            double centroidPeakWidth = 0;        // No centroiding
            int centroidThisScan = 0; // No centroiding
            DateTime creationDate = DateTime.MinValue;

            var scanPath = Path.Combine(fpath, $"{spec.Id}.cdf");

            var cdf = DataSet.Open(scanPath);
            cdf.IsAutocommitEnabled = false;
            AddMetadata(cdf);
            IXRawfile5 rawFile = (IXRawfile5)new MSFileReader_XRawfile();
            rawFile.Open(spec.RawFilePath);
            rawFile.SetCurrentController(nControllerType, nContorllerNumber);
            rawFile.GetNumSpectra(ref totalNumScans);

            rawFile.GetFirstSpectrumNumber(ref firstScanNumber);
            rawFile.GetLastSpectrumNumber(ref lastScanNumber);
            rawFile.GetCreationDate(ref creationDate);


            var massVar = cdf.Add<double>("mass_values", new[] { "point_number" });
            var intensities = cdf.Add<Int32>("intensity_values", new[] { "point_number" });
            var acqTimeVar = cdf.Add<double>("scan_acquisition_time", new[] { "scan_number" });
            var scIndexVar = cdf.Add<int>("scan_index", new[] { "scan_number" });
            var scCountVar = cdf.Add<int>("point_count", new[] { "scan_number" });
            var totalIntensityVar = cdf.Add<double>("total_intensity", new[] { "scan_number" });
            massVar.Metadata["scale_factor"] = 1.0;
            intensities.Metadata["scale_factor"] = 1.0;

            try
            {
                var retTimes = new double[lastScanNumber];
                var ticArr = new double[lastScanNumber];
                var scanIndex = new Int32[lastScanNumber];
                var pointCount = new Int32[lastScanNumber];

                for (int curScanNum = firstScanNumber; curScanNum <= lastScanNumber; curScanNum++)
                {
                    rawFile.GetScanHeaderInfoForScanNum(curScanNum,
                    ref numDataPoints,
                    ref retentionTimeInMinutes,
                    ref minObservedMZ,
                    ref maxObservedMZ,
                    ref totalIonCurrent,
                    ref basePeakMZ,
                    ref basePeakIntensity,
                    ref channel, // unused
                    ref uniformTime, // unused
                    ref frequency // unused
                );
                    scanFilter = null;
                    rawFile.GetFilterForScanNum(curScanNum, ref scanFilter);

                    peakFlags = null;
                    centroidPeakWidth = 0;
                    arraySize = 0;
                    intensityCutoffType = 1;
                    intensityCutoffValue = (int)(0.001 * basePeakIntensity);
                    rawFile.GetMassListFromScanNum(
                        ref curScanNum,
                        szFilter,             // filter
                        intensityCutoffType, // intensityCutoffType
                        intensityCutoffValue, // intensityCutoffValue
                        maxNumberOfPeaks,     // maxNumberOfPeaks
                        centroidThisScan,        // centroid result?
                        ref centroidPeakWidth,    // centroidingPeakWidth
                        ref rawData,        // daw data
                        ref peakFlags,        // peakFlags
                        ref arraySize);        // array size

                    ;
                    var datArray = (Array)rawData;
                    var massArr = new double[arraySize];
                    var intArr = new int[arraySize];
                    for (var j = 0; j < arraySize; j++)
                    {
                        massArr[j] = (double)datArray.GetValue(0, j);
                        intArr[j] = Convert.ToInt32(datArray.GetValue(1, j));
                    }
                    //Console.Write($"curScan = {curScanNum}\n");
                    var ind = curScanNum - 1;
                    pointCount[ind] = arraySize;
                    if (ind == 0)
                    {
                        scanIndex[ind] = 0;
                        scanIndex[ind + 1] = arraySize;
                    }
                    else
                    {
                        scanIndex[ind] += scanIndex[ind - 1];
                        if (ind + 1 < lastScanNumber)
                            scanIndex[ind + 1] = arraySize;
                    }
                    retTimes[ind] = TimeSpan.FromMinutes(retentionTimeInMinutes).TotalSeconds;
                    ticArr[ind] = totalIonCurrent;
                    massVar.Append(massArr);
                    intensities.Append(intArr);
                    rawData = null;
                }

                totalIntensityVar.PutData(ticArr);
                scIndexVar.PutData(scanIndex);
                acqTimeVar.PutData(retTimes);
                scCountVar.PutData(pointCount);

                cdf.Commit();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            finally
            {
                cdf.Dispose();
                rawFile.Close();
            }

        }
        private void AddMetadata(DataSet cdf)
        {
            cdf.Metadata["created_by"] = "Raw2CDF 1.0";
            cdf.Metadata["netcdf_revision"] = "4.0.1.0";
            cdf.Metadata["source_file_format"] = "Finnigan";
            cdf.Metadata["netcdf_file_date_time_stamp"] = GetDateTimeFormatted(DateTime.Now);
        }
        private string GetDateTimeFormatted(DateTime dt)
        {
            var offset = dt - dt.ToUniversalTime();
            return dt.ToString($"yyyyMMddHHmmss+{offset.ToString("hhmm", CultureInfo.InvariantCulture)}", CultureInfo.InvariantCulture);
        }
    }
}
