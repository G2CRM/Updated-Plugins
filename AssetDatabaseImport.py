"""
AssetDatabaseImport

Only imports the assets to the Asset and Structure Asset tables.
"""

import csv
import datetime
import sqlite3
import struct
import sys

from osgeo import ogr
from PyQt5.QtCore import *
from PyQt5.QtGui import *
from PyQt5.QtWidgets import *


class AssetImport(QObject):
    finished = pyqtSignal()
    progress = pyqtSignal()
    setupProgress = pyqtSignal(int)

    def __init__(self, assetShapefile, database, projection):
        QObject.__init__(self)
        self.assetShapefile = assetShapefile
        # 1. OPTIMIZATION: Use a set instead of a list for O(1) lookups
        self.assetExtRefSet = set()
        self.database = database
        self.projection = projection
        self.occupancyTypes = set()
        self.foundationTypes = set()
        self.constructionTypes = set()

    def StartImport(self):
        try:
            self.connection = sqlite3.connect(self.database)
            self.connection.enable_load_extension(True)
            self.connection.load_extension("mod_spatialite")
            self.connection.enable_load_extension(False)
            self.connection.execute("PRAGMA foreign_keys=ON")
            self.cursor = self.connection.cursor()
            self.numProcessed = 0
            self.assetLayer = self.OpenAssetShapefile()
            self.DeleteAssets()
            self.ImportAssets()
            self.FixSpatialProperties()
            self.cursor.close()
            self.finished.emit()
            self.connection.commit()
            QMessageBox.information(
                None,
                "Asset Import Complete",
                "Asset shapefile has been successfully imported into the database.",
            )
        except Exception as e:
            print(str(e))
            self.connection.rollback()
            QMessageBox.critical(
                None,
                "Asset Import Error",
                "There was an error during import. The following is an automatically generated description of the error:  %s"
                % (str(e)),
            )
        finally:
            try:
                self.connection.close()
            except Exception:
                pass

    def verifyDateFormat(self, date):
        date = datetime.datetime.strptime(date, "%Y-%m-%d")
        return date

    def FixSpatialProperties(self):
        SQL = "SELECT DisableSpatialIndex ('Assets', 'AssetRepresentativePointGeometry'); DROP TABLE IF EXISTS idx_Assets_AssetRepresentativePointGeometry; SELECT CreateSpatialIndex('Assets', 'AssetRepresentativePointGeometry'); SELECT UpdateLayerStatistics('Assets', 'AssetRepresentativePointGeometry');"
        self.cursor.executescript(SQL)

    def OpenAssetShapefile(self):
        driver = ogr.GetDriverByName("ESRI Shapefile")
        dataSource = driver.Open(str(self.assetShapefile), 0)
        return dataSource.GetLayer()

    def ImportAssets(self):
        driver = ogr.GetDriverByName("ESRI Shapefile")
        dataSource = driver.Open(str(self.assetShapefile), 0)
        assetLayer = dataSource.GetLayer()

        spatialRef = assetLayer.GetSpatialRef()
        if spatialRef != None:
            spatialRefAuthorityName = spatialRef.GetAuthorityName(None)
            spatialRefCode = spatialRef.GetAuthorityCode(None)

        if spatialRefCode == None:
            spatialRefCode = str(self.projection)

        self.setupProgress.emit(int(assetLayer.GetFeatureCount()))
        self.GetForeignKeys()
        for feature in assetLayer:
            assetId = self.ImportSingleAsset(feature, spatialRefCode)
            if assetId != None:
                self.ImportStructureAsset(feature, assetId)
                self.numProcessed += 1
            self.progress.emit()

    def RemoveComma(self, string):
        return string.replace(",", "")

    def RemoveNewLine(self, string):
        return string.replace("\n", "")

    def DeleteAssets(self):
        self.cursor.execute("Delete from AssetEPZCorrespondence")
        self.cursor.execute("Delete from AssetMACorrespondence")
        self.cursor.execute("Delete from StructureAsset")
        self.cursor.execute("DELETE FROM Assets")
        return

    """
        TODO:   Add error message dialog output for duplicated AssetExternalReference
    """

    def ImportSingleAsset(self, feature, spatialRefCode):
        OccupancyType = self.RemoveNewLine(feature.GetField("Occup_Type"))
        AssetDescription = self.RemoveNewLine(
            self.RemoveComma(feature.GetField("A_Descrip"))
        )
        assetGeom = feature.GetGeometryRef()
        assetGeom.FlattenTo2D()

        if spatialRefCode != None:
            AssetPointGeometry = (
                "Transform(PointFromText('"
                + str(assetGeom.ExportToWkt())
                + "', "
                + spatialRefCode
                + "), 4326)"
            )
        else:
            AssetPointGeometry = (
                "PointFromText('" + str(assetGeom.ExportToWkt()) + "', 4326)"
            )

        AssetExternalReference = self.RemoveNewLine(feature.GetField("Extern_Ref"))

        # 3. OPTIMIZATION: O(1) set lookup instead of O(N) list search
        if AssetExternalReference in self.assetExtRefSet:
            print(
                "AssetExternalReference: %s already exists in data set"
                % AssetExternalReference
            )
            return None
        self.assetExtRefSet.add(AssetExternalReference)

        AssetType = feature.GetField("AssetType")
        AssetActive = int(feature.GetField("Active")) == 1
        AssetPolygon = None
        AssetLine = None
        DateOnline = self.verifyDateFormat(feature.GetField("DateOnline"))
        DateOffline = self.verifyDateFormat(feature.GetField("DateOfflin"))

        SQL = (
            "INSERT INTO Assets (AssetExternalReference, AssetDescription, AssetType, AssetActive, DateOnline, DateOffline, AssetRepresentativePointGeometry, AssetPolygonGeometry, AssetLineGeometry) VALUES (?, ?, ?, ?, ?, ?, "
            + AssetPointGeometry
            + ", ?, ?);"
        )
        self.cursor.execute(
            SQL,
            [
                AssetExternalReference,
                AssetDescription,
                AssetType,
                AssetActive,
                DateOnline,
                DateOffline,
                AssetPolygon,
                AssetLine,
            ],
        )

        # 4. OPTIMIZATION: Avoid the extra SELECT query entirely by using lastrowid
        return self.cursor.lastrowid

    def GetForeignKeys(self):
        # 2. OPTIMIZATION: Store foreign keys in sets for faster validation check
        FoundationSQL = "SELECT FoundationType FROM FoundationType"
        self.foundationTypes = set(
            elt[0] for elt in self.cursor.execute(FoundationSQL).fetchall()
        )
        ConstructionSQL = "SELECT ConstructionType FROM ConstructionType"
        self.constructionTypes = set(
            elt[0] for elt in self.cursor.execute(ConstructionSQL).fetchall()
        )
        OccupancySQL = "SELECT OccupancyType FROM OccupancyType"
        self.occupancyTypes = set(
            elt[0] for elt in self.cursor.execute(OccupancySQL).fetchall()
        )

    def ImportStructureAsset(self, feature, AssetID):
        Description = self.RemoveComma(feature.GetField("A_Descrip"))
        FoundationType = self.RemoveNewLine(feature.GetField("Found_Type"))
        ConstructionType = self.RemoveNewLine(feature.GetField("Const_Type"))
        OccupancyType = self.RemoveNewLine(feature.GetField("Occup_Type"))
        StructureValueP1 = feature.GetField("S_Value_P1")
        StructureValueP2 = feature.GetField("S_Value_P2")
        StructureValueP3 = feature.GetField("S_Value_P3")
        ContentsValueP1 = feature.GetField("C_Value_P1")
        ContentsValueP2 = feature.GetField("C_Value_P2")
        ContentsValueP3 = feature.GetField("C_Value_P3")
        DepreciationFactor = feature.GetField("Depr_Fact")
        Width = feature.GetField("Width")
        Length = feature.GetField("Length")
        FoundationHeight = feature.GetField("Found_Ht")
        GroundElevation = feature.GetField("G_Elev")
        FirstFloorElevationP1 = feature.GetField("FF_Elev_P1")
        FirstFloorElevationP2 = feature.GetField("FF_Elev_P2")
        FirstFloorElevationP3 = feature.GetField("FF_Elev_P3")
        NumberOfFloors = feature.GetField("Num_Floors")
        TimeToRebuildP1 = feature.GetField("RebTime_P1")
        TimeToRebuildP2 = feature.GetField("RebTime_P2")
        TimeToRebuildP3 = feature.GetField("RebTime_P3")
        NumberOfTimesRebuildingAllowed = feature.GetField("N_Rebuilds")
        PopulationNightUnder65 = feature.GetField("Pop_N_U65")
        PopulationDayUnder65 = feature.GetField("Pop_D_U65")
        PopulationNight65AndOver = feature.GetField("Pop_N_65")
        PopulationDay65AndOver = feature.GetField("Pop_D_65")
        WaveDamageActive = int(feature.GetField("WaveDamage")) != 0
        IsInBenefitsBase = int(feature.GetField("InBeneBase")) != 0
        TargetFirstFloorElevation = feature.GetField("TargFstFlr")
        RaisingCostPerFoot = feature.GetField("RaiseCstFt")
        CumulativeDamageThreshold = feature.GetField("CumDmgThld")
        PostRaisingStructureValueP1 = feature.GetField("PR_SVal_P1")
        PostRaisingStructureValueP2 = feature.GetField("PR_SVal_P2")
        PostRaisingStructureValueP3 = feature.GetField("PR_SVal_P3")
        PostRaisingContentsValueP1 = feature.GetField("PR_CVal_P1")
        PostRaisingContentsValueP2 = feature.GetField("PR_CVal_P2")
        PostRaisingContentsValueP3 = feature.GetField("PR_CVal_P3")
        PostRaisingTimeToRebuildP1 = feature.GetField("PR_RebT_P1")
        PostRaisingTimeToRebuildP2 = feature.GetField("PR_RebT_P2")
        PostRaisingTimeToRebuildP3 = feature.GetField("PR_RebT_P3")

        if ConstructionType not in self.constructionTypes:
            print(ConstructionType + " not found in: " + str(self.constructionTypes))
            raise ValueError(
                "Foreign key violation. Invalid construction type ("
                + ConstructionType
                + ') for asset "'
                + Description
                + '"'
            )
        elif FoundationType not in self.foundationTypes:
            print(FoundationType + " not found in: " + str(self.foundationTypes))
            raise ValueError(
                "Foreign key violation. Invalid foundation type ("
                + FoundationType
                + ') for asset "'
                + Description
                + '"'
            )
        elif OccupancyType not in self.occupancyTypes:
            print(OccupancyType + " not found in: " + str(self.occupancyTypes))
            raise ValueError(
                "Foreign key violation. Invalid occupancy type ("
                + OccupancyType
                + ') for asset "'
                + Description
                + '"'
            )

        SQL = "INSERT INTO StructureAsset (AssetID, FoundationType, ConstructionType, OccupancyType, Description, StructureValueP1, StructureValueP2, StructureValueP3, ContentsValueP1, ContentsValueP2, ContentsValueP2, ContentsValueP3, DepreciationFactor, Width, Length, FoundationHeight, GroundElevation, FirstFloorElevationP1, FirstFloorElevationP2, FirstFloorElevationP3, NumberOfFloors, TimeToRebuildP1, TimeToRebuildP2, TimeToRebuildP3, NumberOfTimesRebuildingAllowed, PopulationNightUnder65, PopulationDayUnder65, PopulationNight65AndOver, PopulationDay65AndOver, IsInBenefitsBase, TargetFirstFloorElevation, RaisingCostPerFoot, CumulativeDamageThreshold, PostRaisingStructureValueP1, PostRaisingStructureValueP2, PostRaisingStructureValueP3, PostRaisingContentsValueP1, PostRaisingContentsValueP2, PostRaisingContentsValueP3, PostRaisingTimeToRebuildP1,PostRaisingTimeToRebuildP2,PostRaisingTimeToRebuildP3) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        self.cursor.execute(
            SQL,
            [
                AssetID,
                FoundationType,
                ConstructionType,
                OccupancyType,
                Description,
                StructureValueP1,
                StructureValueP2,
                StructureValueP3,
                ContentsValueP1,
                ContentsValueP2,
                ContentsValueP2,
                ContentsValueP3,
                DepreciationFactor,
                Width,
                Length,
                FoundationHeight,
                GroundElevation,
                FirstFloorElevationP1,
                FirstFloorElevationP2,
                FirstFloorElevationP3,
                NumberOfFloors,
                TimeToRebuildP1,
                TimeToRebuildP2,
                TimeToRebuildP3,
                NumberOfTimesRebuildingAllowed,
                PopulationNightUnder65,
                PopulationDayUnder65,
                PopulationNight65AndOver,
                PopulationDay65AndOver,
                IsInBenefitsBase,
                TargetFirstFloorElevation,
                RaisingCostPerFoot,
                CumulativeDamageThreshold,
                PostRaisingStructureValueP1,
                PostRaisingStructureValueP2,
                PostRaisingStructureValueP3,
                PostRaisingContentsValueP1,
                PostRaisingContentsValueP2,
                PostRaisingContentsValueP3,
                PostRaisingTimeToRebuildP1,
                PostRaisingTimeToRebuildP2,
                PostRaisingTimeToRebuildP3,
            ],
        )
