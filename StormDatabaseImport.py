'''
StormDatabaseImport
'''

## This Code was modified 08/04/2021 to facilatate the import of H5 files regardless of ASCII vs UTF-8 string type
##  Modifications/Additions are indicated with "##" & noted with "Code Addition".

## All modifications were initially made and tested in StormDatabaseImportSTWAVE. The modifications were replecated in this script for ADCIRC H5 import routine.



from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *
import tables as tb
from datetime import datetime
from dateutil.parser import *
import sys
from osgeo import ogr
import sqlite3
import math
import constant
import logging
logger = logging.getLogger(__name__)

class StormDatabaseImport(QObject):

    # emit this signal when progress has been made towards the progress bar
    progress = pyqtSignal(int, int)

    # progress bar setup signal: num items, title
    progressSetup = pyqtSignal(int, str)

    finished = pyqtSignal()
    warning = pyqtSignal()

    # function that converts all dates of the form "yyyymmddHHMM" to datetime objects in a given list
    def fixDate(self, x):
        if math.isnan(x):
            try:
                return parse(str(x))
            except:
                logger.warning(f'Unable to parse {x} as datetime using dateutil.parser.parse')
                return -1
        else:
            try:
                return datetime.strptime(str(int(x)), '%Y%m%d%H%M')
            except:
                logger.warning(f'Unable to parse {x} as datetime in format \%Y\%m\%d\%H\%M')
                return x

    def __init__(self, dbfile, filenames, stormsToUse, mssTextId, mssDesc, vDatumConv, vDatumTideConv, useWaveInfo, studyId, searchBounds, managerDb, mssBasisYear):
        super(QObject,self).__init__()
        self.dbfile = dbfile
        self.total = 0
        self.fileNames = filenames
        self.stormsToUse = stormsToUse
        self.mssTextId = mssTextId
        self.mssDesc = mssDesc
        self.vDatumConv = vDatumConv
        self.vDatumTideConv = vDatumTideConv
        self.useWaveInfo = useWaveInfo
        self.studyId = studyId
        self.searchBounds = searchBounds
        self.managerDb = managerDb
        self.stormsLeftToUse = list(stormsToUse)
        self.missingStorms = []
        self.mssBasisYear = mssBasisYear

    def StartImport(self):
        logger.debug("Inside StartImport")
        logger.debug(f"Trying to connect to {self.dbfile}")
        self.connection = sqlite3.connect(self.dbfile)
        logger.debug("Trying to load mod_spatialite")
        self.connection.enable_load_extension(True)
        self.connection.load_extension("mod_spatialite")
        self.connection.enable_load_extension(False)
        logger.debug("Trying to open cursor")
        self.cursor = self.connection.cursor()

        logger.debug("Trying to import storm set")
        self.ImportModeledStormSet()
        logger.debug("Storm Set Import Complete!")
        total = len(self.fileNames)
        # self.progressSetup.emit(total, "Processing HDF5 files...")

        summaryId = 1

        summaryIdSql = "SELECT IFNULL(MAX(ModeledStormLocationID), 0) FROM ModeledStormLocationSummary"
        self.cursor.execute(summaryIdSql)
        summaryId += int(self.cursor.fetchone()[0])

        stormNames = []
        for file in self.fileNames:
            # Open the HDF5 file
            h5File = tb.open_file(str(file), mode='r')
            # Get some file attributes
            
            ## Code Addition ## (If statement to determine if the H5 data is type bytes or str. Need to "decode" if the values is type bytes. Default for else)
            if isinstance(h5File.get_node_attr('/', 'Save Point ID'),bytes):
                savePointID = str(int(h5File.get_node_attr('/', 'Save Point ID').decode("utf-8")))
            else: 
                savePointID = str(int(h5File.get_node_attr('/', 'Save Point ID')))
            ##
            
            ## Code Addition ## (If statement to determine if the H5 data is type bytes or str. Need to "decode" if the values is type bytes. Default for else)
            if isinstance(h5File.get_node_attr('/', 'Save Point Latitude'),bytes):
                lat = str(h5File.get_node_attr('/', 'Save Point Latitude').decode("utf-8"))
            else: 
                lat = str(h5File.get_node_attr('/', 'Save Point Latitude'))
            ##
            
            ## Code Addition ## (If statement to determine if the H5 data is type bytes or str. Need to "decode" if the values is type bytes. Default for else)
            if isinstance(h5File.get_node_attr('/', 'Save Point Longitude'),bytes):
                lon = str(h5File.get_node_attr('/', 'Save Point Longitude').decode("utf-8"))
            else: 
                lon = str(h5File.get_node_attr('/', 'Save Point Longitude'))
            ##
            
            sys.stdout.write("Importing ADCIRC for Save Point {save} at ({lat}, {lon}) from file {file}\n".format(save=savePointID, lat=lat, lon=lon, file=file))
            logger.debug(f"Importing ADCIRC for Save Point {savePointID} at ({lat}, {lon}) from file {file}\n")
            
            # Save location data
            self.ImportLocation(int(savePointID), lon, lat)
            logger.debug("Storm Location Import for %s Complete!", savePointID)

            idx = 0
            numStorms = len(h5File.list_nodes("/"))
            self.progressSetup.emit(numStorms + 1, "Processing HDF5 files...")
            
            # Loop through all storms in the HDF5 file
            for storm in h5File.list_nodes("/"):

                # Setup time gathering
                prevDate = None
                timeHours = 0.0
                # Get storm name and lists
                stormNodeName = storm._v_name
                
                ## Code Addition ## (If statement to determine if the H5 data is type bytes or str. Need to "decode" if the values is type bytes. Default for else)
                if isinstance(h5File.get_node_attr('/' + stormNodeName, 'Storm Name'),bytes):
                    stormName = str(h5File.get_node_attr('/' + stormNodeName, 'Storm Name').decode("utf-8"))
                else: 
                    stormName = str(h5File.get_node_attr('/' + stormNodeName, 'Storm Name'))                
                ##

                
                if (len(self.stormsToUse) > 0 and stormNodeName not in self.stormsToUse):
                    continue
                try:
                    self.stormsLeftToUse.remove(stormNodeName)
                except:
                    pass
                
                ## Code Addition ## Check if the first value in the water elevation array is 'nan'. Continue to next value in the for loop without storing the storm name & data.
                if str(storm._f_get_child('Water Elevation')[0]) == 'nan':
                    logger.debug("NaN value detected in 'Water Elevation'. Storm skipped in the H5 import process. Storm: %s", stormName)
                    continue
                ##
    
                
                
                
                logger.debug("Importing Storm %s", stormName)
                elevData = storm._f_get_child('Water Elevation')
                logger.debug("Water Elevation: %s", elevData)
                dateData = list(map(self.fixDate, storm._f_get_child('yyyymmddHHMM')))
                logger.debug("Date: %s", dateData)

                stormNumberSql = "SELECT MAX(StormNumber), COUNT(StormNumber) FROM Storms WHERE StormIdentifier = ?"

                self.cursor.execute(stormNumberSql, (stormName,))
                stormNumberResult = self.cursor.fetchone()
                if stormNumberResult[1] >= 1:
                    stormNumber = stormNumberResult[0]
                else:
                    self.cursor.execute("SELECT ifnull(MAX(StormNumber), 0) FROM Storms")
                    stormNumber = 1 + self.cursor.fetchone()[0]
                    
                ## Code Addition ## No code was modified here. Worth noting that str/bytes being pulled and then converted to float. Not issue with Fox Point but keep an eye if plugin breaks down in other cases.
                stormInterval = float(h5File.get_node_attr('/' + stormNodeName, 'Record Interval'))
                ## 
                
                
                    ## Code Addition ## (If statement to determine if the H5 data is type bytes or str. Need to "decode" if the values is type bytes. Default for else)
                if isinstance(h5File.get_node_attr('/' + stormNodeName, 'Record Interval Units'),bytes):
                    stormIntervalUnits = str(h5File.get_node_attr('/' + stormNodeName, 'Record Interval Units').decode("utf-8"))
                else: 
                    stormIntervalUnits = str(h5File.get_node_attr('/' + stormNodeName, 'Record Interval Units'))                                
                    ##
                
                
                if (stormIntervalUnits.lower() in constant.HOURS_ABBR_LIST):
                    stormInterval = stormInterval * constant.SECONDS_IN_HOUR # Convert to seconds
                    stormIntervalUnits = 'hr'
                    logger.debug("%s interpretted as hours for %s", stormIntervalUnits, stormName)
                elif (stormIntervalUnits.lower() in constant.SECONDS_ABBR_LIST):
                    stormInterval = stormInterval # Leave as seconds
                    stormIntervalUnits = 'sec'
                    logger.debug("%s interpretted as seconds for %s", stormIntervalUnits, stormName)
                elif (stormIntervalUnits.lower() in constant.MINUTES_ABBR_LIST):
                    stormInterval = stormInterval * constant.SECONDS_IN_MINUTE #Convert minutes to seconds
                    stormIntervalUnits = 'min'
                    logger.debug("%s interpretted as minutes for %s", stormIntervalUnits, stormName)
                else:
                    stormInterval = stormInterval * constant.SECONDS_IN_MINUTE #Convert minutes to seconds
                    stormIntervalUnits = 'min'
                    logger.debug("%s interpretted as minutes for %s", stormIntervalUnits, stormName)
                    minute_list = ",".join(constant.MINUTES_ABBR_LIST)
                    hour_list = ",".join(constant.HOURS_ABBR_LIST)
                    second_list = ",".join(constant.SECONDS_ABBR_LIST)
                    sys.stderr.write(f"WARNING: 'Record Interval Units' {stormIntervalUnits} for {stormName} is not a known unit, defaulting to minutes!{os.linesep}Accepted values are seconds ({second_list}), minutes ({minute_list}), and hours ({hour_list})")
                    logger.warning(f"'Record Interval Units' {stormIntervalUnits} for {stormName} is not a known unit, defaulting to minutes!{os.linesep}Accepted values are seconds ({second_list}), minutes ({minute_list}), and hours ({hour_list})")
                # Statistics setup
                timeStepCounter = -1
                timeMaxSurge = 0
                maxTimeStep = 0
                minSurge = 9999999.0
                maxSurge = -999999.0

                # Only import storm and modeled storm if it has not been added already
                if (stormName not in stormNames):
                    stormNames.append(stormName)
                    
                    
                        ## Code Addition ## (If statement to determine if the H5 data is type bytes or str. Need to "decode" if the values is type bytes. Default for else)
                    if isinstance(h5File.get_node_attr('/' + stormNodeName, 'Storm Type'),bytes):
                        stormTypeRaw = str(h5File.get_node_attr('/' + stormNodeName, 'Storm Type').decode("utf-8"))
                    else: 
                        stormTypeRaw = str(h5File.get_node_attr('/' + stormNodeName, 'Storm Type'))                                
                        ##                      
                    
                    logger.debug("Storm has Storm Type %s", stormTypeRaw)
                    stormType = ''
                    logger.debug("Storm Type: %s", stormType)
                    if (stormTypeRaw.lower() in constant.EXTRA_TROPICAL_ABBR_LIST):
                        stormType = 'ET'
                        sys.stdout.write(f"DEBUG: {stormTypeRaw} interpretted as Extratropical ('ET') for {stormName}")
                        logger.debug(f"{stormTypeRaw} interpretted as Extratropical ('ET') for {stormName}")
                    elif (stormTypeRaw.lower() in constant.TROPICAL_ABBR_LIST):
                        stormType = 'T'
                        sys.stdout.write(f"DEBUG: {stormTypeRaw} interpretted as Tropical ('T') for {stormName}")
                        logger.debug(f"{stormTypeRaw} interpretted as Tropical ('T') for {stormName}")
                    else:
                        stormType = 'T'
                        sys.stderr.write(f"WARNING: 'Storm Type' {stormTypeRaw} is not recognized, defaulting {stormName} to Tropical ('T')")
                        sys.stdout.write(f"DEBUG: {stormTypeRaw} interpretted as Tropical ('T') for {stormName}")
                        logger.warning(f"'Storm Type' {stormTypeRaw} is not recognized, defaulting {stormName} to Tropical ('T')")
                        logger.debug(f"{stormTypeRaw} interpretted as Tropical ('T') for {stormName}")
                    try:
                        self.ImportStorms(stormNumber, stormName, 0.0, stormType, 0,0,0,0,dateData[0], 0, True, self.mssTextId, datetime.datetime.now().year)
                    except Exception:
                        logger.error(f"Error encountered importing storm {stormName}", exc_info=True)

                # Check if location/storm already imported
                testSql = "SELECT COUNT(ModeledStormLocationID) FROM ModeledStormLocationSummary WHERE StormNumber = ? AND LocationID = ?"
                self.cursor.execute(testSql, (stormNumber, int(savePointID), ))
                if self.cursor.fetchone()[0] >= 1:
                    sys.stdout.write("WARNING: Skipping storm {snum} at {locid}. Already exists in database.\n".format(snum=stormNumber, locid=int(savePointID)))
                    logger.debug(f"Skipping storm {stormNumber} at {savePointID}. Already exists in database")
                else:
                    logger.debug(f"Begin loading storm {stormNumber} at {savePointID}.")
                    # Loop through all data
                    for date, elev in zip(dateData, elevData):
                        if(isinstance(date, datetime)):
                            date = date
                            logger.debug("Is datetime object")
                        else:
                            logger.debug("Is not datetime object")
                            if(prevDate is None):
                                logger.debug("prevDate was None")
                                date = datetime(self.mssBasisYear, 1, 1)
                                logger.debug(f'date is now parsed as {date}')
                            else:
                                date = prevDate + timedelta(seconds=stormInterval)
                                logger.debug(f'date is now parsed as {date}')
                        timeStepCounter += 1
                        stormDate = date
                        # Get time hours
                        if (prevDate is not None):
                            timeHours += (stormDate - prevDate).seconds / 3600.0 # Get Hours from seconds as a double
                        prevDate = stormDate
                        data = elev

                        # Correct for 'nan' values
                        if (str(data) == 'nan' or data <= -9999.0):
                            data = -99999.0
                        else:
                            data *= 3.28084 # Convert to feet
                            data += self.vDatumConv # Add Vertical Datum Conversion
                        
                        try:
                            self.ImportStormDetail(summaryId, timeHours, timeStepCounter, data)
                            logger.debug(f"Storm detail imported for {summaryId}, step {timeStepCounter} at {timeHours} hours.")
                        except:
                            logger.error(f"Error encountered importing storm detail {summaryId} step {timeStepCounter} at {timeHours} hours.", exc_info=True)

                        # Calculate statistics
                        if (data > maxSurge):
                            maxSurge = data
                            timeMaxSurge = timeHours
                        if (data < minSurge):
                            minSurge = data
                    logger.debug("Importing Summary...")
                    self.ImportStormSummary(timeStepCounter, timeHours, minSurge, maxSurge, timeMaxSurge, stormNumber, summaryId, int(savePointID))
                    logger.debug(f"Imported Summary for {timeStepCounter} at {timeHours} hours and location {savePointID}")
                    summaryId += 1
                    idx += 1
                    self.progress.emit(idx, numStorms + 1)
            self.progress.emit(numStorms + 1, numStorms + 1)
            h5File.close()
            logger.debug("Closed HDF5 file")
        self.fixGeometries()
        logger.debug("Fixed Geometries")
        self.addLocalTideStations()
        logger.debug("Added local Tide Stations")
        self.checkExistingStorms()
        logger.debug("Checked Existing Storms")
        self.connection.commit()
        logger.debug("Saved Storms Database")
        self.connection.close()
        logger.debug("Closed Storms Database")
        self.finished.emit()

    def checkExistingStorms(self):
        SQL = 'SELECT StormIdentifier FROM Storms WHERE StormIdentifier NOT IN (\'' + '\',\''.join(self.stormsToUse) + '\')'
        sys.stdout.write(SQL)
        results = self.cursor.execute(SQL)
        self.missingStorms = [item[0] for item in results]
        return
        
    def addLocalTideStations(self):
        self.cursor.execute("DELETE FROM LocalTideStations")
        self.cursor.execute("ATTACH DATABASE '" + self.managerDb + "' as manager")
        SQL = "INSERT INTO LocalTideStations SELECT * from TideStations WHERE TideStationId IN (SELECT TideStationId FROM TideStations ts JOIN manager.Study WHERE StudyId = '" + self.studyId + "' AND within(ts.Geometry, ST_Expand(Transform(PolyFromText(ST_AsText(BoundingRectangle), DataProjectionId), 4326), " + self.searchBounds + ")))"
        self.cursor.execute(SQL)
        
    def fixGeometries(self):
        disableScript = "SELECT DisableSpatialIndex ('Location', '{field}')"
        dropScript = "DROP TABLE IF EXISTS idx_Location_{field}"
        createScript = "SELECT CreateSpatialIndex('Location', '{field}')"
        updateScript = "SELECT UpdateLayerStatistics('Location', '{field}')"

        indexCursor = self.connection.cursor()

        sys.stdout.write("Updating spatial indexes\n")

        try:
            indexCursor.execute(disableScript.format(field="LocationRepresentativePointGeometry"))
            indexCursor.execute(dropScript.format(field="LocationRepresentativePointGeometry"))
        except RuntimeError as e:
            sys.stderr.write("DisableSpatialIndex: {exp}\n".format(exp = e.message))
        except:
            sys.stderr.write('Error executing DisableSpatialIndex on {field}\n'.format(field="LocationRepresentativePointGeometry"))
        try:
            indexCursor.execute(createScript.format(field="LocationRepresentativePointGeometry"))
            indexCursor.execute(updateScript.format(field="LocationRepresentativePointGeometry"))
        except RuntimeError as e:
            sys.stderr.write("CreateSpatialIndex: {exp}\n".format(exp = e.message))
        except:
            sys.stderr.write('Error creating spatial index on {field}\n'.format(field="LocationRepresentativePointGeometry"))

        try:
            indexCursor.execute(disableScript.format(field="LocationAreaOfInfluenceGeometry"))
            indexCursor.execute(dropScript.format(field="LocationAreaOfInfluenceGeometry"))
        except RuntimeError as e:
            sys.stderr.write("DisableSpatialIndex: {exp}\n".format(exp = e.message))
        except:
            sys.stderr.write('Error executing DisableSpatialIndex on {field}\n'.format(field="LocationAreaOfInfluenceGeometry"))
        try:
            indexCursor.execute(createScript.format(field="LocationAreaOfInfluenceGeometry"))
            indexCursor.execute(updateScript.format(field="LocationAreaOfInfluenceGeometry"))
        except RuntimeError as e:
            sys.stderr.write("CreateSpatialIndex: {exp}\n".format(exp = e.message))
        except:
            sys.stderr.write('Error creating spatial index on {field}\n'.format(field="LocationAreaOfInfluenceGeometry"))

    def ImportModeledStorm(self, stormNumber):
        sys.stdout.write("Importing ModeledStorm {storm}\n".format(storm=stormNumber,))
        existingStormSql = "SELECT COUNT(*) FROM ModeledStorm WHERE ModeledStormSetTextID = ? and StormNumber = ?"
        self.cursor.execute(existingStormSql, (self.mssTextId,stormNumber))
        if self.cursor.fetchone()[0] < 1:
            SQL = "INSERT INTO ModeledStorm (ModeledStormSetTextID, StormNumber) VALUES (?, ?);"
            self.cursor.execute(SQL, (self.mssTextId, stormNumber))
            logger.debug(f"ModeledStorm with StormNumber {stormNumber} has been inserted")
        else:
            sys.stdout.write("ModeledStorm with StormNumber {stormNumber} already imported.\n".format(stormNumber=stormNumber,))
            logger.debug(f"ModeledStorm with StormNumber {stormNumber} already imported.")

    def ImportModeledStormSet(self):
        logger.debug("Inside ImportModeledStormSet!")
        logger.debug("Importing Modeled Storm Set {storm_set}\n".format(storm_set=self.mssTextId,))
        sys.stdout.write("Importing Modeled Storm Set {storm_set}\n".format(storm_set=self.mssTextId,))
        existingModeledStormSetSql = "SELECT COUNT(ModeledStormSetTextID) FROM ModeledStormSet WHERE ModeledStormSetTextID = ?"
        self.cursor.execute(existingModeledStormSetSql, (self.mssTextId,))
        if self.cursor.fetchone()[0] >= 1:
            existingModeledStormSetSql = "SELECT UseWaveInfo FROM ModeledStormSet WHERE ModeledStormSetTextID = ?"
            useWaveInfo = self.cursor.execute(existingModeledStormSetSql, (self.mssTextId,)).fetchone()[0] == 1
            if useWaveInfo != self.useWaveInfo:
                self.warning.emit()
            sys.stdout.write("ModeledStormSet with ID {mss} already imported\n".format(mss=self.mssTextId,))
            return
        SQL = "INSERT INTO ModeledStormSet (ModeledStormSetTextID, ModeledStormSetDescription, UseWaveInfo, SLCReferenceYear) VALUES (?, ?, ?, ?);"
        self.cursor.execute(SQL, (self.mssTextId, self.mssDesc, self.useWaveInfo, self.mssBasisYear))
        return

    def ImportStorms(self, stormnum, stormID, prob, type, emonth, eday, lmonth, lday, stormDate, stormDuration, stormActive, msstId, year):
        sys.stdout.write("Importing Storm {storm}\n".format(storm=stormnum,))
        logger.debug(f'Importing Storm {stormnum}, with ID {stormID} at {stormDate}')
        stormDateTime = stormDate
        if(isinstance(stormDateTime, datetime) == False):
            stormDateTime = datetime(year, 1, 1)
        existingStormSql = "SELECT COUNT(StormNumber) FROM Storms WHERE StormIdentifier = ?"
        self.cursor.execute(existingStormSql, (stormID,))
        if self.cursor.fetchone()[0] < 1:
            SQL = "INSERT INTO Storms VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);"
            self.cursor.execute(SQL, (stormnum, stormID, prob, type, emonth, eday, lmonth, lday, stormDateTime, stormDuration, stormActive))
            logger.debug(f"Added new storm with ID {stormID}")
        else:
            sys.stdout.write("Storm with Storm ID {storm} already imported\n".format(storm=stormID,))
            logger.debug(f"Storm with Storm ID {stormID} already imported")
        self.ImportModeledStorm(stormnum)
        logger.debug(f"Imported Modeled Storm with ID {stormID} as {stormnum}")



    def ImportLocation(self, savePointID, lon, lat):
        sys.stdout.write("Importing Location {loc}\n".format(loc=savePointID,))
        existingLocationSql = "SELECT COUNT(LocationID) FROM Location WHERE LocationID = ?"
        self.cursor.execute(existingLocationSql, (savePointID,))
        if self.cursor.fetchone()[0] >= 1:
            sys.stdout.write("Location with LocationID {loc} already imported.\n".format(loc=savePointID,))
            return
        point = ogr.Geometry(ogr.wkbPoint)
        point.AddPoint(float(lon), float(lat))
        point.FlattenTo2D()
        buffer = point.Buffer(.015)
        buffer.FlattenTo2D()
        geomWKT = "PointFromText('" +  str(point.ExportToWkt()) + "', 4326)"
        bufText = "PolyFromText('" +  str(buffer.ExportToWkt()) + "', 4326)"
        locationSQL = ("INSERT INTO Location ("
                       " LocationID,"
                       " LocationExternalReference,"
                       " UseTidalAdjustment,"
                       " TideStation1ID,"
                       " TideStation1DatumAdjustment,"
                       " TideStation2ID,"
                       " TideStation2DatumAdjustment, "
                       " TideInterpolationFactor,"
                       " LocationRepresentativePointGeometry,"
                       " LocationAreaOfInfluenceGeometry)"
                       " VALUES (?, ?, 0, NULL, ?, NULL, ?, 0.0, " + geomWKT + ", " + bufText + ")")
        locationExternalId = str(savePointID)
        self.cursor.execute(locationSQL, (savePointID, locationExternalId, self.vDatumTideConv, self.vDatumTideConv))


    def ImportStormDetail(self, summaryId, timeHours, timeStepCounter, surge):
        SQL = "INSERT INTO ModeledStormLocationDetail (ModeledStormLocationID, TimeHours, TimeStepCounter, Surge) VALUES (?, ?, ?, ?)"
        self.cursor.execute(SQL, (summaryId, timeHours, timeStepCounter, surge))

    def ImportStormSummary(self,  maxTimeStep, maxTimeHours, minSurge, maxSurge, timeMaxSurge, stormNum, summaryId, locId):
        SQL = "INSERT INTO ModeledStormLocationSummary (ModeledStormLocationID, StormNumber, MaximumNumberOfTimeSteps, MaximumTimeHours, MinimumSurge, MaximumSurge, TimeOfPeakSurge, LocationID) VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
        self.cursor.execute(SQL, (summaryId, stormNum, maxTimeStep, maxTimeHours, minSurge, maxSurge, timeMaxSurge, locId))
