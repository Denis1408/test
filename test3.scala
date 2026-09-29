//Main

import org.apache.spark.sql.types._

// Crash-level tables
val CrashReport = TableSpec(explodes = Seq.empty, cols = Seq(
  ColSpec("Crash_Id", "CrashReport.Header.CrashNumber"),
  ColSpec("CrashNumber", "CrashReport.Header.CrashNumber"),
  ColSpec("RecordNum", "CrashReport.Header.CrashRecord.RecordNum", IntegerType),
  ColSpec("RecordStatus", "CrashReport.Header.CrashRecord.RecordStatus"),
  ColSpec("RecordModified", "CrashReport.Header.CrashRecord.RecordModified", StringType), // varchar(100)
  ColSpec("RecordModifiedBy", "CrashReport.Header.CrashRecord.RecordModifiedBy"),
  ColSpec("DocNum", "CrashReport.CrashLevel.Document.DocNum"),
  ColSpec("FormName", "CrashReport.CrashLevel.Document.FormName"),
  ColSpec("FormType", "CrashReport.CrashLevel.Document.FormType.Code"),
  ColSpec("ReportableFlag", "CrashReport.CrashLevel.Document.ReportableFlag", BooleanType),
  ColSpec("AmendedFlag", "CrashReport.CrashLevel.Document.AmendedFlag", BooleanType),
  ColSpec("LinkedDocNum", "CrashReport.CrashLevel.LinkedDocNum"),// not found
  ColSpec("Location", "CrashReport.CrashLevel.CrashClass.Location.Code"),
  ColSpec("Jurisdiction", "CrashReport.CrashLevel.CrashClass.Jurisdiction.Code"),
  ColSpec("RelationTrwy", "CrashReport.CrashLevel.CrashClass.RelationTrwy.Code"),
  ColSpec("CrashDate", "CrashReport.CrashLevel.CrashDate"),
  ColSpec("CrashTime", "CrashReport.CrashLevel.CrashTime"),
  ColSpec("CrashMonth", "CrashReport.CrashLevel.CrashMonth.Code"), 
  ColSpec("NotifiedDate", "CrashReport.CrashLevel.NotifiedDate"),
  ColSpec("NotifiedTime", "CrashReport.CrashLevel.NotifiedTime"),
  ColSpec("ReportStartDate", "CrashReport.CrashLevel.ReportStartDate"),
  ColSpec("ReportStartTime", "CrashReport.CrashLevel.ReportStartTime"),
  ColSpec("TotalUnits", "CrashReport.CrashLevel.CrashTotals.TotalUnits", IntegerType),
  ColSpec("TotalVehicles", "CrashReport.CrashLevel.CrashTotals.TotalVehicles", IntegerType),
  ColSpec("TotalMotorists", "CrashReport.CrashLevel.CrashTotals.TotalMotorists", IntegerType),
  ColSpec("TotalNonMotorists", "CrashReport.CrashLevel.CrashTotals.TotalNonMotorists", IntegerType),
  ColSpec("TotalInjured", "CrashReport.CrashLevel.CrashTotals.TotalInjured", IntegerType),
  ColSpec("TotalFatalities", "CrashReport.CrashLevel.CrashTotals.TotalFatalities", IntegerType),
  ColSpec("NonReportableFatalityFlag", "CrashReport.CrashLevel.NonReportableFatalityFlag", BooleanType),
  ColSpec("NonReportableFatalityComment", "CrashReport.CrashLevel.NonReportableFatalityComment"),// not found
  ColSpec("DayOfWeek", "CrashReport.CrashLevel.DayOfWeek.Code"),
  ColSpec("CrashSeverity", "CrashReport.CrashLevel.CrashSeverity.Code"),
  ColSpec("InjurySeverity", "CrashReport.CrashLevel.InjurySeverity.Code"),
  ColSpec("AccessControl", "CrashReport.CrashLevel.AccessControl.Code"),
  ColSpec("FirstHarmfulEvent", "CrashReport.CrashLevel.FirstHarmfulEvent.Code"),
  ColSpec("FirstHarmfulEventLocation", "CrashReport.CrashLevel.FirstHarmfulEventLocation.Code"),
  ColSpec("MannerOfCollision", "CrashReport.CrashLevel.MannerOfCollision.Code"),
  ColSpec("LightCondition", "CrashReport.CrashLevel.LightCondition.Code"),
  ColSpec("IntersectionType", "CrashReport.CrashLevel.IntersectionType.Code"),
  ColSpec("SchoolBusRelated", "CrashReport.CrashLevel.SchoolBusRelated.Code"),
  ColSpec("AlcoholInvolved", "CrashReport.CrashLevel.AlcoholInvolved.Code"),
  ColSpec("DrugInvolved", "CrashReport.CrashLevel.DrugInvolved.Code"),
  ColSpec("RelationToJunction_WithinICArea", "CrashReport.CrashLevel.RelationToJunction.WithinICArea.Code"),
  ColSpec("RelationToJunction_SpecificLocation", "CrashReport.CrashLevel.RelationToJunction.SpecificLocation.Code"),  
  ColSpec("ReconstructedBy", "CrashReport.CrashLevel.ReconstructedBy"),//not found
  ColSpec("PhotosBy", "CrashReport.CrashLevel.PhotosBy"),
  ColSpec("SRCaseNumber", "CrashReport.CrashLevel.SRCaseNumber"),//not found
  ColSpec("WorkZoneFlag", "CrashReport.CrashLevel.WorkZone.WorkZoneFlag", BooleanType),
  ColSpec("WorkZoneDetails_CrashLocation", "CrashReport.CrashLevel.WorkZoneDetails.CrashLocation", IntegerType),//not found
  ColSpec("WorkZoneDetails_WorkZoneType", "CrashReport.CrashLevel.WorkZoneDetails.WorkZoneType", IntegerType),//not found
  ColSpec("WorkZoneDetails_WorkZoneTypeOther", "CrashReport.CrashLevel.WorkZoneDetails.WorkZoneTypeOther"),//not found
  ColSpec("WorkZoneDetails_WorkersPresent", "CrashReport.CrashLevel.WorkZoneDetails.WorkersPresent", IntegerType),//not found
  ColSpec("WorkZoneDetails_LawEnfPresent", "CrashReport.CrashLevel.WorkZoneDetails.LawEnfPresent", IntegerType),//not found
  ColSpec("WorkZoneDetails_WorkZoneSpeedLimit", "CrashReport.CrashLevel.WorkZoneDetails.WorkZoneSpeedLimit", IntegerType),//not found
  ColSpec("WorkZoneDetails_AdvRegSpeedLimit", "CrashReport.CrashLevel.WorkZoneDetails.AdvRegSpeedLimit", IntegerType),//not found
  ColSpec("WorkZoneDetails_NormalPostedSpeed", "CrashReport.CrashLevel.WorkZoneDetails.NormalPostedSpeed", IntegerType),//not found
  ColSpec("SecondaryCrash_SecondaryCrashFlag", "CrashReport.CrashLevel.SecondaryCrash.SecondaryCrashFlag", BooleanType),
  ColSpec("SecondaryCrash_PrimaryCrashDocNum", "CrashReport.CrashLevel.SecondaryCrash.PrimaryCrashDocNum"),
  ColSpec("SecondaryCrash_PrimaryAgencyName", "CrashReport.CrashLevel.SecondaryCrash.PrimaryAgencyName"),
  ColSpec("ArrivalDate", "CrashReport.CrashLevel.CrashScene.ArrivalDate"),
  ColSpec("ArrivalTime", "CrashReport.CrashLevel.CrashScene.ArrivalTime"),
  ColSpec("LaneClosureFlag", "CrashReport.CrashLevel.CrashScene.LaneClosureFlag", BooleanType),
  ColSpec("ClosureType", "CrashReport.CrashLevel.CrashScene.ClosureType.Code"),
  ColSpec("ClosedDate", "CrashReport.CrashLevel.CrashScene.ClosedDate"),
  ColSpec("ClosedTime", "CrashReport.CrashLevel.CrashScene.ClosedTime"),
  ColSpec("OpenedDate", "CrashReport.CrashLevel.CrashScene.OpenedDate"),
  ColSpec("OpenedTime", "CrashReport.CrashLevel.CrashScene.OpenedTime"),
  ColSpec("ClearedDate", "CrashReport.CrashLevel.CrashScene.ClearedDate"),
  ColSpec("ClearedTime", "CrashReport.CrashLevel.CrashScene.ClearedTime"),
  ColSpec("TotalWitnesses", "CrashReport.CrashLevel.CrashScene.TotalWitnesses", IntegerType),// not found
  ColSpec("AgencyOfficerNumber", "CrashReport.CrashLevel.LawEnforcement.AgencyOfficerNumber"),
  ColSpec("AgencyJurisdiction", "CrashReport.CrashLevel.LawEnforcement.AgencyJurisdiction"),
  ColSpec("AgencyNumber", "CrashReport.CrashLevel.LawEnforcement.AgencyNumber"),
  ColSpec("AgencyName", "CrashReport.CrashLevel.LawEnforcement.AgencyName"),
  ColSpec("AgencyType", "CrashReport.CrashLevel.LawEnforcement.AgencyType.Code"),
  ColSpec("AgencyCaseID", "CrashReport.CrashLevel.LawEnforcement.AgencyCaseID"),
  ColSpec("AgencySpace", "CrashReport.CrashLevel.LawEnforcement.AgencySpace"),// not found
  ColSpec("AgencyCrashNumber", "CrashReport.CrashLevel.LawEnforcement.AgencyCrashNumber"),
  ColSpec("AgencyAddress_AddressLine1", "CrashReport.CrashLevel.LawEnforcement.AgencyAddress.AddressLine1"),
  ColSpec("AgencyAddress_AddressLine2", "CrashReport.CrashLevel.LawEnforcement.AgencyAddress.AddressLine2"),
  ColSpec("AgencyAddress_City", "CrashReport.CrashLevel.LawEnforcement.AgencyAddress.City"),
  ColSpec("AgencyAddress_State", "CrashReport.CrashLevel.LawEnforcement.AgencyAddress.State"),
  ColSpec("AgencyAddress_Zipcode", "CrashReport.CrashLevel.LawEnforcement.AgencyAddress.Zipcode"),
  ColSpec("AgencyAddress_POBox", "CrashReport.CrashLevel.LawEnforcement.AgencyAddress.POBox"),
  ColSpec("AgencyAddress_Country", "CrashReport.CrashLevel.LawEnforcement.AgencyAddress.Country"),
  ColSpec("AgencyPhone_PhoneNumber", "CrashReport.CrashLevel.LawEnforcement.AgencyPhone.PhoneNumber"),
  ColSpec("AgencyPhone_Extension", "CrashReport.CrashLevel.LawEnforcement.AgencyPhone.Extension"),
  
  // Officer Information
  ColSpec("OffficerName_LastName", "CrashReport.CrashLevel.LawEnforcement.Officer.OffficerName.LastName", StringType),
  ColSpec("OffficerName_FirstName", "CrashReport.CrashLevel.LawEnforcement.Officer.OffficerName.FirstName", StringType),
  ColSpec("OffficerName_MiddleName", "CrashReport.CrashLevel.LawEnforcement.Officer.OffficerName.MiddleName", StringType),
  ColSpec("OffficerName_Suffix", "CrashReport.CrashLevel.LawEnforcement.Officer.OffficerName.Suffix"),
  ColSpec("BadgeNumber", "CrashReport.CrashLevel.LawEnforcement.Officer.BadgeNumber", StringType),
  ColSpec("LEAOfficerID", "CrashReport.CrashLevel.LawEnforcement.Officer.LEAOfficerID", StringType),
  ColSpec("DNROfficerID", "CrashReport.CrashLevel.LawEnforcement.Officer.DNROfficerID", StringType),
   
  // Officer Narrative
  ColSpec("OfficerNarrative", "CrashReport.OfficerNarrative", StringType)
))

val Additional = TableSpec(
  explodes = Seq("CrashReport.CrashLevel.AdditionalFormsOrData.Additional"),
  carry = Seq(
    ColSpec("Crash_Id", "CrashReport.Header.CrashNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

val AnimalType = TableSpec(
  explodes = Seq("CrashReport.CrashLevel.AnimalTypes.AnimalType"),
  carry = Seq(
    ColSpec("Crash_Id", "CrashReport.Header.CrashNumber", StringType)
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

val ClosureReason = TableSpec(
  explodes = Seq("CrashReport.CrashLevel.CrashScene.ClosureReasons.ClosureReason"),
  carry = Seq(
    ColSpec("Crash_Id", "CrashReport.Header.CrashNumber", StringType)
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

val CrashDiagram = TableSpec(
  explodes = Seq(
    "CrashReport.CrashReportImage"                   // row0 = crash level (1:1 with crash)
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber", StringType)
  ),
  cols = Seq(
    ColSpec("ImageFileName", "ImageFile.ImageFileName", StringType),
    ColSpec("ImageFileType", "ImageFile.ImageFileType", StringType),
    ColSpec("ImageFileSize", "ImageFile.ImageFileSize", StringType),
    ColSpec("ImageFileData", "ImageFile.ImageFileData", StringType)
  )
)

val CrashReportImage = TableSpec(
  explodes = Seq("CrashReport.CrashReportImage"),
  carry = Seq(
    ColSpec("Crash_Id", "CrashReport.Header.CrashNumber", StringType)
  ),
  cols = Seq(
    ColSpec("ImageNumber", "ImageNumber"),
    ColSpec("ReleaseLevel", "ReleaseLevel", IntegerType),
    ColSpec("ImageFileName", "ImageFile.ImageFileName", StringType),
    ColSpec("ImageFileType", "ImageFile.ImageFileType", StringType),
    ColSpec("ImageFileSize", "ImageFile.ImageFileSize", StringType),
    ColSpec("ImageFileData", "ImageFile.ImageFileData", StringType)
  )
)

val CrashLocation = TableSpec(explodes = Seq("CrashReport.CrashLevel.CrashLocation"), carry = Seq(
  ColSpec("Crash_Id", "CrashReport.Header.CrashNumber")
), cols = Seq(
  ColSpec("County_CountyCode", "County.CountyCode", IntegerType),
  ColSpec("County_CountyName", "County.CountyName"),
  ColSpec("County_CountyFIPS", "County.CountyFIPS", IntegerType),
  ColSpec("Municipality_MuniCode", "Municipality.MuniCode", IntegerType),
  ColSpec("Municipality_MuniName", "Municipality.MuniName"),
  ColSpec("Municipality_MuniType", "Municipality.MuniType"),
  ColSpec("DTSDRegion_RegionNmbr", "DTSDRegion.RegionNmbr"),
  ColSpec("DTSDRegion_RegionName", "DTSDRegion.RegionName"),
  ColSpec("BOTSRegion_RegionNmbr", "BOTSRegion.RegionNmbr"),
  ColSpec("BOTSRegion_RegionName", "BOTSRegion.RegionName"),
  ColSpec("STPLPost_PostNmbr", "STPLPost.PostNmbr"),
  ColSpec("STPLPost_PostName", "STPLPost.PostName"),
  ColSpec("Tribal_TribalCode", "Tribal.TribalCode", IntegerType),
  ColSpec("Tribal_TribalName", "Tribal.TribalName"),
  ColSpec("GeoCoords_Latitude", "GeoCoords.Latitude"),
  ColSpec("GeoCoords_Longitude", "GeoCoords.Longitude"),
  ColSpec("GeoCoords_Source", "GeoCoords.Source.Code"),
  ColSpec("OnStreet", "OnStreet"),
  ColSpec("OnHighway_HighwayName", "OnHighway.HighwayName"),
  ColSpec("OnHighway_HighwaySysType", "OnHighway.HighwaySysType"),
  ColSpec("OnHighway_HighwayDirection", "OnHighway.HighwayDirection"),
  ColSpec("OnHighway_HighwayType", "OnHighway.HighwayType"),
  ColSpec("IntersectionDistance", "IntersectionDistance"),
  ColSpec("IntersectionDirection", "IntersectionDirection"),
  ColSpec("AtStreet", "AtStreet"),
  ColSpec("AtHighway_HighwayName", "AtHighway.HighwayName"),
  ColSpec("AtHighway_HighwaySysType", "AtHighway.HighwaySysType"),
  ColSpec("AtHighway_HighwayDirection", "AtHighway.HighwayDirection"),
  ColSpec("AtHighway_HighwayType", "AtHighway.HighwayType"),
  ColSpec("StructureType", "StructureType.Code"),
  ColSpec("StructureNumber", "StructureNumber")
))

val EnvironmentalFactor = TableSpec(
  explodes = Seq("CrashReport.CrashLevel.EnvironmentalFactors.EnvironmentalFactor"), 
  carry = Seq(
  ColSpec("Crash_Id", "CrashReport.Header.CrashNumber")
), cols = Seq(
  //ColSpec("Id", "Id", IntegerType),
  ColSpec("Code", "Code")
))

val FatalSupplement = TableSpec(
  explodes = Seq(
    "FatalSupplement"               // row0 = fatal supplement crash level
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber", StringType)
  ),
  cols = Seq(
    ColSpec("RecordNum", "Header.CrashRecord.RecordNum", IntegerType),
    ColSpec("RecordModified", "Header.CrashRecord.RecordModified", TimestampType),
    ColSpec("RecordModifiedBy", "Header.CrashRecord.RecordModifiedBy", StringType),
    ColSpec("FormDocNum", "CrashLevel.Document.FormDocNum", StringType),
    ColSpec("FormName", "CrashLevel.Document.FormName", StringType),
    ColSpec("ReportDate", "CrashLevel.ReportDate", StringType),
    ColSpec("AgencyCaseID", "AgencyCaseID", StringType),
    ColSpec("AgencySpace", "AgencySpace", StringType),
    ColSpec("OffficerName_LastName", "Officer.OffficerName.LastName", StringType),
    ColSpec("OffficerName_FirstName", "Officer.OffficerName.FirstName", StringType),
    ColSpec("OffficerName_MiddleName", "Officer.OffficerName.MiddleName", StringType),
    ColSpec("OffficerName_Suffix", "Officer.OffficerName.Suffix", StringType),
    ColSpec("BadgeNumber", "Officer.BadgeNumber", StringType),
    ColSpec("LEAOfficerID", "Officer.LEAOfficerID", StringType),
    ColSpec("DNROfficerID", "Officer.DNROfficerID", StringType),
    ColSpec("AgencyAddress_AddressLine1", "AgencyAddress.AddressLine1", StringType),
    ColSpec("AgencyAddress_AddressLine2", "AgencyAddress.AddressLine2", StringType),
    ColSpec("AgencyAddress_City", "AgencyAddress.City", StringType),
    ColSpec("AgencyAddress_State", "AgencyAddress.State", StringType),
    ColSpec("AgencyAddress_Zipcode", "AgencyAddress.Zipcode", StringType),
    ColSpec("AgencyAddress_POBox", "AgencyAddress.POBox", StringType),
    ColSpec("AgencyAddress_Country", "AgencyAddress.Country", StringType),
    ColSpec("AgencyPhone_PhoneNumber", "AgencyPhone.PhoneNumber", StringType),
    ColSpec("AgencyPhone_Extension", "AgencyPhone.Extension", StringType)
  )
)

val FatalSupplement_Unit = TableSpec(
  explodes = fatalsupplementunitPath,
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber", StringType)
    
  ),
  cols = Seq(
    ColSpec("FatalSupplement_Unit_Id", "UnitNumber", IntegerType),
    ColSpec("UnitNumber", "UnitNumber", IntegerType),
    ColSpec("AttemptedAvoidanceManeuver", "AttemptedAvoidanceManeuver.Code"),
    ColSpec("AttemptedAvoidanceManeuverOtherAction", "AttemptedAvoidanceManeuverOtherAction", StringType),
    ColSpec("ComplianceWithLicenseRestrictions", "ComplianceWithLicenseRestrictions.Code"),
    ColSpec("Pre-ImpactStability", "Pre-ImpactStability.Code"),
    ColSpec("Pre-ImpactStabilityOtherVehicleLossofControl", "Pre-ImpactStabilityOtherVehicleLossofControl.Code", StringType),
    ColSpec("MethodOfAlcoholDeterminationByPolice", "MethodOfAlcoholDeterminationByPolice.Code"),
    ColSpec("MethodOfDrugDeterminationByPolice", "MethodOfDrugDeterminationByPolice.Code"),
    ColSpec("Pre-ImpactLocation", "Pre-ImpactLocation.Code"),
    ColSpec("EstimatedTravelSpeed", "EstimatedTravelSpeed.Code")
  )
)

val FatalSupplement_FatalReportImage = TableSpec(
  explodes = Seq("FatalSupplement.FatalReportImage"),
  carry = Seq(
    ColSpec("Crash_Id", "CrashReport.Header.CrashNumber", StringType)
  ),
  cols = Seq(
    ColSpec("ImageNumber", "ImageNumber"),
    ColSpec("ReleaseLevel", "ReleaseLevel"),
    ColSpec("ImageFileName", "ImageFile.ImageFileName", StringType),
    ColSpec("ImageFileType", "ImageFile.ImageFileType", StringType),
    ColSpec("ImageFileSize", "ImageFile.ImageFileSize", StringType),
    ColSpec("ImageFileData", "ImageFile.ImageFileData", StringType),
    ColSpec("ImageFileDataMerged", "ImageFile.ImageFileDataMerged", StringType)
  )
)

val FatalSupplement_Person = TableSpec(
explodes = fatalsupplementunitPath :+ "Persons.Person",
carry = Seq(
ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
//ColSpec("UnitNumber", "^row0.UnitNumber"),
ColSpec("FatalSupplement_Unit_Id", "^row0.UnitNumber", IntegerType)
//ColSpec("UnitPersonNumber", "^row0.UnitPersonNumber", IntegerType)
),
cols = Seq(
//ColSpec("Id", "Id", IntegerType),
ColSpec("UnitPersonNumber", "UnitPersonNumber", IntegerType),
ColSpec("Name_LastName",  "Name.LastName"),
ColSpec("Name_FirstName", "Name.FirstName"),
ColSpec("Name_MiddleName",  "Name.MiddleName"),
ColSpec("Name_Suffix", "Name.Suffix"),
ColSpec("DOB", "DOB"),
ColSpec("Sex", "Sex"),
ColSpec("LocationofDeath", "LocationofDeath.Code"),
ColSpec("AmbulanceNotification", "AmbulanceNotification", IntegerType),
ColSpec("AmbulanceArrivedatScene", "AmbulanceArrivedatScene", IntegerType),
ColSpec("AmbulanceArrivedatHospital", "AmbulanceArrivedatHospital", IntegerType),
ColSpec("DateOfDeath", "DateOfDeath"),
ColSpec("TimeOfDeath", "TimeOfDeath")
))


val UnitTbl = TableSpec(
  explodes = unitPath,
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber")
  ),
  cols = Seq(
    ColSpec("Unit_Id", "UnitNumber", IntegerType),  
    ColSpec("UnitNumber", "UnitNumber", IntegerType),
    ColSpec("UnitType", "UnitType.Code"),
    ColSpec("UnitStatus", "UnitStatus.Code"),
    ColSpec("UnitTotals_TotalPersons", "UnitTotals.TotalPersons", IntegerType),
    ColSpec("UnitTotals_TotalCitations", "UnitTotals.TotalCitations", IntegerType),
    ColSpec("UnitTotals_TotalFatalities", "UnitTotals.TotalFatalities", IntegerType),
    ColSpec("UnitTotals_TotalInjured", "UnitTotals.TotalInjured", IntegerType),
    ColSpec("UnitTotals_TotalUninjured", "UnitTotals.TotalUninjured", IntegerType)
  )
)



val WeatherCondition = TableSpec(explodes = Seq("CrashReport.CrashLevel.WeatherConditions.WeatherCondition"), carry = Seq(
  ColSpec("Crash_Id", "CrashReport.Header.CrashNumber")
), cols = Seq(
  //ColSpec("Id", "Id", IntegerType),
  ColSpec("Code", "Code")
))

val RoadSurfaceCondition = TableSpec(explodes = Seq("CrashReport.CrashLevel.RoadSurfaceConditions.RoadSurfaceCondition"), carry = Seq(
  ColSpec("Crash_Id", "CrashReport.Header.CrashNumber")
), cols = Seq(
  //ColSpec("Id", "Id", IntegerType),
  ColSpec("Code", "Code")
))

val Vehicle = TableSpec(explodes = Seq("CrashReport.UnitLevel.Unit"),
  carry = Seq(
  ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
  ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
  ColSpec("Vehicle_Id", "^row0.UnitNumber")
), cols = Seq(
  ColSpec("VehicleNumber", "Vehicle.VehicleNumber"),
  ColSpec("VehicleType", "Vehicle.VehicleType.Code"),
  ColSpec("VIN", "Vehicle.VehicleDetails.VIN"),
  ColSpec("LicensePlate_PlateNumber", "Vehicle.VehicleDetails.LicensePlate.PlateNumber"),
  ColSpec("LicensePlate_PlateType", "Vehicle.VehicleDetails.LicensePlate.PlateType"),
  ColSpec("LicensePlate_State", "Vehicle.VehicleDetails.LicensePlate.State"),
  ColSpec("LicensePlate_Country", "Vehicle.VehicleDetails.LicensePlate.Country"),
  ColSpec("VehicleMake", "Vehicle.VehicleDetails.VehicleMake"),
  ColSpec("VehicleModel", "Vehicle.VehicleDetails.VehicleModel"),
  ColSpec("VehicleModelYear", "Vehicle.VehicleDetails.VehicleModelYear"),
  ColSpec("VehicleBodyStyle", "Vehicle.VehicleDetails.VehicleBodyStyle"),
  ColSpec("VehicleColor", "Vehicle.VehicleDetails.VehicleColor"),
  ColSpec("AVFlag", "Vehicle.AVFlag", BooleanType),
  ColSpec("SpecialFunction", "Vehicle.SpecialFunction.Code"),
  ColSpec("EmergencyUse", "Vehicle.EmergencyUse.Code"),
  ColSpec("SpeedLimit", "Vehicle.SpeedLimit", IntegerType),
  ColSpec("TravelDirection", "Vehicle.TravelDirection.Code"),
  ColSpec("TrafficWay", "Vehicle.TrafficWay.Code"),
  ColSpec("TotalLanes", "Vehicle.TotalLanes", IntegerType),
  ColSpec("Curvature", "Vehicle.Roadway.Curvature.Code"),
  ColSpec("Grade", "Vehicle.Roadway.Grade.Code"),
  ColSpec("TrafficControl_TypeTCD", "Vehicle.TrafficControl.TypeTCD.Code"),
  ColSpec("TrafficControl_InopMissing", "Vehicle.TrafficControl.InopMissing.Code"),
  ColSpec("SurfaceType", "Vehicle.SurfaceType.Code"),
  ColSpec("VehicleAction_PriorAction", "Vehicle.VehicleAction.PriorAction.Code"),
  ColSpec("VehicleAction_PriorActionOther", "Vehicle.VehicleAction.PriorActionOther.Code"),
  ColSpec("InitialContactPoint", "Vehicle.VehicleDamage.InitialContactPoint.Code"),
  ColSpec("ExtentOfDamage", "Vehicle.VehicleDamage.ExtentOfDamage.Code"),
  ColSpec("PreTireCrashMark", "Vehicle.VehicleDamage.PreTireCrashMark", IntegerType),//not found
  ColSpec("SequenceOfEvents_Event1", "Vehicle.SequenceOfEvents.Event1.Code"),
  ColSpec("SequenceOfEvents_Event2", "Vehicle.SequenceOfEvents.Event2.Code"),
  ColSpec("SequenceOfEvents_Event3", "Vehicle.SequenceOfEvents.Event3.Code"),
  ColSpec("SequenceOfEvents_Event4", "Vehicle.SequenceOfEvents.Event4.Code"),
  ColSpec("MostHarmfulEvent", "Vehicle.MostHarmfulEvent.Code"),
  ColSpec("BusUse", "Vehicle.BusUse.Code"),
  ColSpec("VehicleTowed", "Vehicle.VehicleTowed.Code"),
  ColSpec("VehicleTowedBy", "Vehicle.VehicleTowedBy"),
  ColSpec("PolicyHolder_HolderSameAsOwner", "Vehicle.Insurance.PolicyHolder.HolderSameAsOwner", BooleanType),
  ColSpec("PolicyHolder_PolicyHolderType", "Vehicle.Insurance.PolicyHolder.PolicyHolderType.Code"),
  ColSpec("PolicyHolder_CompanyGovt", "Vehicle.Insurance.PolicyHolder.CompanyGovt"),
  ColSpec("PolicyHolder_LastName", "Vehicle.Insurance.PolicyHolder.LastName"),
  ColSpec("PolicyHolder_FirstName", "Vehicle.Insurance.PolicyHolder.FirstName"),
  ColSpec("Company_CompanyCode", "Vehicle.Insurance.Company.CompanyCode"),
  ColSpec("Company_CompanyName", "Vehicle.Insurance.Company.CompanyName"),
  ColSpec("TotalHazMatTypes", "Vehicle.TotalHazMatTypes", IntegerType),
  ColSpec("TrailerFlag", "Vehicle.TrailerFlag", BooleanType),
  ColSpec("TotalTrailers", "Vehicle.TotalTrailers", IntegerType)
))


val VehicleOwner = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",           // row0 = each unit
    "Vehicle.VehicleOwners.VehicleOwner"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Vehicle_Id", "^row0.UnitNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("OwnerSameAsDriver", "OwnerSameAsDriver", BooleanType),
    ColSpec("OwnerType", "OwnerDetails.OwnerType.Code"),
    ColSpec("OwnerNumber", "OwnerDetails.OwnerNumber", StringType),
    ColSpec("DMVCustNumber", "OwnerDetails.DMVCustNumber", StringType),
    ColSpec("CompanyGovtName", "CompanyGovtName", StringType),
    ColSpec("OwnerName_LastName", "OwnerDetails.OwnerName.LastName", StringType),
    ColSpec("OwnerName_FirstName", "OwnerDetails.OwnerName.FirstName", StringType),
    ColSpec("OwnerName_MiddleName", "OwnerDetails.OwnerName.MiddleName", StringType),
    ColSpec("OwnerName_Suffix", "OwnerDetails.OwnerName.Suffix", StringType),
    ColSpec("OwnerAddress_AddressLine1", "OwnerDetails.OwnerAddress.AddressLine1", StringType),
    ColSpec("OwnerAddress_AddressLine2", "OwnerDetails.OwnerAddress.AddressLine2", StringType),
    ColSpec("OwnerAddress_City", "OwnerDetails.OwnerAddress.City", StringType),
    ColSpec("OwnerAddress_State", "OwnerDetails.OwnerAddress.State", StringType),
    ColSpec("OwnerAddress_Zipcode", "OwnerDetails.OwnerAddress.Zipcode", StringType),
    ColSpec("OwnerAddress_POBox", "OwnerDetails.OwnerAddress.POBox", StringType),
    ColSpec("OwnerAddress_Country", "OwnerDetails.OwnerAddress.Country", StringType),
    ColSpec("OwnerPhone_PhoneNumber", "OwnerDetails.OwnerPhone.PhoneNumber", StringType),
    ColSpec("OwnerPhone_Extension", "OwnerDetails.OwnerPhone.Extension", StringType),
    ColSpec("OwnerDOB", "OwnerDOB", StringType),
    ColSpec("Insurance", "Insurance.Code")
  )
)

val Citation = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",           // row0 = each unit
    "Persons.Person",                       // row1 = each person in unit
    "AllPersons.CitationInfo.Citations",          // row2 = citations container
    "Citation"                                     // row3 = individual citation
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),    // Get UnitNumber from exploded unit
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Person_Id","^row1.UnitPersonNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("StatuteNumber", "StatuteNumber", StringType),
    ColSpec("StatuteSeverityDesc", "StatuteSeverityDesc", StringType),
    ColSpec("CitationNumber", "CitationNumber", StringType),
    ColSpec("RestrictedFlag", "RestrictedFlag", BooleanType)
  )
)

val Person = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",           // row0 = each unit
    "Persons.Person"                         // row1 = each person in unit
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber", StringType),  // ^ = absolute path
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Person_Id", "^row1.UnitPersonNumber", StringType),
    ColSpec("PersonNumber", "^row1.PersonNumber", StringType)  // Carry PersonNumber for child table joins
  ),
  cols = Seq(
    // Basic Person Info
    ColSpec("UnitPersonNumber", "UnitPersonNumber", IntegerType),
    ColSpec("DMVCustNumber", "DMVCustNumber", StringType),
    ColSpec("PersonType", "AllPersons.PersonType.Code"),
    ColSpec("DriverFlag", "AllPersons.DriverFlag", BooleanType),
    ColSpec("PedFlag", "AllPersons.PedFlag", BooleanType),
    ColSpec("OnDuty", "AllPersons.OnDuty.Code", StringType),
   
    // Name fields
    ColSpec("Name_LastName", "AllPersons.Name.LastName", StringType),
    ColSpec("Name_FirstName", "AllPersons.Name.FirstName", StringType),
    ColSpec("Name_MiddleName", "AllPersons.Name.MiddleName", StringType),
    ColSpec("Name_Suffix", "AllPersons.Name.Suffix", StringType),
   
    // Address fields
    ColSpec("Address_AddressLine1", "AllPersons.Address.AddressLine1", StringType),
    ColSpec("Address_AddressLine2", "AllPersons.Address.AddressLine2", StringType),
    ColSpec("Address_City", "AllPersons.Address.City", StringType),
    ColSpec("Address_State", "AllPersons.Address.State", StringType),
    ColSpec("Address_Zipcode", "AllPersons.Address.Zipcode", StringType),
    ColSpec("Address_POBox", "AllPersons.Address.POBox", StringType),
    ColSpec("Address_Country", "AllPersons.Address.Country", StringType),
   
    // Phone fields
    ColSpec("Phone_PhoneNumber", "AllPersons.Phone.PhoneNumber", StringType),
    ColSpec("Phone_Extension", "AllPersons.Phone.Extension", StringType),
   
    // Personal details
    ColSpec("DOB", "AllPersons.DOB", StringType),
    ColSpec("Age", "AllPersons.Age", IntegerType),
    ColSpec("Sex", "AllPersons.Sex.Code", StringType),
    ColSpec("InjurySeverity", "AllPersons.InjurySeverity.Code", StringType),
    ColSpec("DateOfDeath", "AllPersons.DateOfDeath", StringType),
    ColSpec("TimeOfDeath", "AllPersons.TimeOfDeath", StringType),
   
    // Driver License fields
    ColSpec("LicenseJurisdiction", "AllPersons.DriverLicense.LicenseJurisdiction.Code"),
    ColSpec("LicenseSOI", "AllPersons.DriverLicense.LicenseSOI", StringType),
    ColSpec("LicenseCountry", "AllPersons.DriverLicense.LicenseCountry", StringType),
    ColSpec("LicenseNumber", "AllPersons.DriverLicense.LicenseNumber", StringType),
    ColSpec("LicenseType", "AllPersons.DriverLicense.LicenseType.Code"),
    ColSpec("LicenseStatus", "AllPersons.DriverLicense.LicenseStatus.Code"),
    ColSpec("PrimeDIDNO", "AllPersons.DriverLicense.PrimeDIDNO", StringType),
   
    // Citation fields
    ColSpec("TotalCitations", "AllPersons.CitationInfo.TotalCitations", IntegerType),
   
    // Motorist fields
    ColSpec("Motorist_SafetyEquipment", "Motorist.SafetyEquipment.Code"),
    ColSpec("Motorist_AirBag", "Motorist.AirBag.Code"),
    ColSpec("Motorist_Ejected", "Motorist.Ejected.Code"),
    ColSpec("Motorist_EjectionPath", "Motorist.EjectionPath.Code"),
    ColSpec("Motorist_TrappedExtricated", "Motorist.TrappedExtricated.Code"),
   
    // Race and driver details
    ColSpec("Race", "AllPersons.Race.Code", StringType),
    ColSpec("OperatingAsClassfiedClass", "Driver.OperatingAsClassfiedClass.Code", StringType),
    ColSpec("CommercialDriverLicence", "Driver.CommercialDriverLicence", BooleanType),
    ColSpec("SpeedRelated", "Driver.SpeedRelated", BooleanType),
   
    // Alcohol and Drug testing
    ColSpec("AlcoholSuspected", "AllPersons.AlcoholSuspected.Code"),
    ColSpec("AlcoholTest_TestStatus", "AllPersons.AlcoholTest.TestStatus.Code"),
    ColSpec("AlcoholTest_TestType", "AllPersons.AlcoholTest.TestType.Code"),
    ColSpec("AlcoholTest_BACTestResult", "AllPersons.AlcoholTest.BACTestResult", StringType),
    ColSpec("DrugSuspected", "AllPersons.DrugSuspected.Code"),
    ColSpec("DrugTest_TestStatus", "AllPersons.DrugTest.TestStatus.Code"),
    ColSpec("DrugTest_TestType", "AllPersons.DrugTest.TestType.Code"),
    ColSpec("DrugTest_TestResult", "AllPersons.DrugTest.TestResult.Code"),
   
    // Action fields (single values, not arrays)
    ColSpec("ActionPriorToCrash_Action", "NonMotorist.ActionPriorToCrash.Action.Code"),
    ColSpec("ActionPriorToCrash_ToFromSchool", "NonMotorist.ActionPriorToCrash.ToFromSchool.Code"),
    ColSpec("ActionTimeOfCrash_ActionOther", "NonMotorist.ActionTimeOfCrash.ActionOther", StringType),
    ColSpec("LocationTimeOfCrash", "NonMotorist.LocationTimeOfCrash.Code"),
   
    // Transport and medical fields
    ColSpec("StruckByUnitNum", "NonMotorist.StruckByUnitNum", IntegerType),
    ColSpec("TransportSource", "Injured.FirstTransport.TransportSource.Code"),
    ColSpec("EMSAgencyID", "Injured.FirstTransport.EMSAgencyID", IntegerType),
    ColSpec("EMSRunNum", "Injured.FirstTransport.EMSRunNum", StringType),
    ColSpec("MedicalFacility_HospitalName", "Injured.FirstTransport.MedicalFacility.HospitalName", StringType),
    ColSpec("MedicalFacility_HospitalCode", "Injured.FirstTransport.MedicalFacility.HospitalCode", StringType),
   
    // Injury details
    ColSpec("InjuryArea", "Injured.InjuryArea.Code"),
    ColSpec("InjuryDiagnosis", "Injured.InjuryDiagnosis.Code", StringType),
    ColSpec("MAISSeverity", "Injured.MAISSeverity.Code"),
   
    // Motorcycle/Bike specific
    ColSpec("HelmetUse", "NonMotorist.HelmetUse.Code"),
    ColSpec("HelmetCompliance", "NonMotorist.HelmetCompliance.Code"),
    ColSpec("TintCompliance", "Driver.TintCompliance.Code"),
    ColSpec("EyeProtection", "NonMotorist.EyeProtection.Code"),
   
    // Physical measurements
    ColSpec("Height", "AllPersons.Height", IntegerType),
    ColSpec("Weight", "AllPersons.Weight", IntegerType),
   
    // Seating position (expanded fields)
    ColSpec("SeatingPositionRow", "AllPersons.SeatingPosition.SeatingPositionRow.Code"),
    ColSpec("SeatingPositionSeat", "AllPersons.SeatingPosition.SeatingPositionSeat.Code"),
   
    // Distraction fields
    ColSpec("DistractedByAction", "DriverNonMotorist.DriverDistractions.DistractedByAction.Code"),
    ColSpec("DistractedBySource", "DriverNonMotorist.DriverDistractions.DistractedBySource.Code"),
   
    // Fatality flags
    ColSpec("StateFatalityFlag", "AllPersons.StateFatalityFlag", BooleanType),
    ColSpec("FederalFatalityFlag", "AllPersons.FederalFatalityFlag", BooleanType)
  )
)

val Flags = TableSpec(explodes = Seq("CrashReport.CrashLevel.Flags"), carry = Seq(
  ColSpec("Crash_Id", "CrashReport.Header.CrashNumber")
), cols = Seq(
  ColSpec("AutoFlag", "AutoFlag", BooleanType),
  ColSpec("BikeFlag", "BikeFlag", BooleanType),
  ColSpec("CitationFlag", "CitationFlag", BooleanType),
  ColSpec("RestrictedFlag", "RestrictedFlag", BooleanType),
  ColSpec("CMVFlag", "CMVFlag", BooleanType),
  ColSpec("MotorcycleFlag", "MotorcycleFlag", BooleanType),
  ColSpec("MaterialSpill", "MaterialSpill", BooleanType),
  ColSpec("MopedFlag", "MopedFlag", BooleanType),
  ColSpec("PedestrianFlag", "PedestrianFlag", BooleanType),
  ColSpec("PhotoFlag", "PhotoFlag", BooleanType),
  ColSpec("TruckFlag", "TruckFlag", BooleanType),
  ColSpec("WitnessFlag", "WitnessFlag", BooleanType),
  ColSpec("OnDuty", "OnDuty", BooleanType),
  ColSpec("OnEmergency", "OnEmergency", BooleanType),
  ColSpec("HitAndRun", "HitAndRun", BooleanType),
  ColSpec("GovtProperty", "GovtProperty", BooleanType),
  ColSpec("TrailerOrTowed", "TrailerOrTowed", BooleanType),
  ColSpec("TrailerFlag", "TrailerFlag", BooleanType),
  ColSpec("ActiveSchoolZone", "ActiveSchoolZone", BooleanType),
  ColSpec("FireFlag", "FireFlag", BooleanType),
  ColSpec("FMCSAReportable", "FMCSAReportable", BooleanType)
))

// Action Time of Crash Action (from Person level)
val ActionTimeOfCrash_Action = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",
    "Persons.Person",
    "NonMotorist.ActionTimeOfCrash.Actions.Action"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Person_Id","^row1.UnitPersonNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Damage Areas (from Vehicle level)
val DamageArea = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",
    "Vehicle.VehicleDamage.AreasOfDamage.DamageArea"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Vehicle_Id", "^row0.UnitNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Distracted By (from Person level)
val DistractedBy = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",
    "Persons.Person",
    "DriverNonMotorist.DriverDistractions"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Person_Id", "^row1.UnitPersonNumber", IntegerType)
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "DistractedByAction.Code")
  )
)

// Driver Action (from Person level)
val DriverAction = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",
    "Persons.Person",
    "Driver.DriverActions.DriverAction"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Person_Id", "^row1.UnitPersonNumber", IntegerType)
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Driver Maneuvered to Avoid Code (from Fatal Supplement)
val DriverManeuveredtoAvoidCode = TableSpec(
  explodes = Seq(
    "FatalSupplement.Units.Unit",
    "DriverManeuveredtoAvoid.ToAvoid"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("FatalSupplement_Unit_Id", "^row0.UnitNumber", IntegerType)
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Driver Pedestrian Condition (from Person level)
val DriverPedCondition = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",
    "Persons.Person",
    "DriverNonMotorist.DriverPedConditions.Condition"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Person_Id", "^row1.UnitPersonNumber", IntegerType) 
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Drivers Vision Obscured By Code (from Fatal Supplement)
val DriversVisionObscuredByCode = TableSpec(
  explodes = Seq(
    "FatalSupplement.Units.Unit",
    "DriversVisionObscuredBy.ObscuredBy"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("FatalSupplement_Unit_Id", "^row0.UnitNumber", IntegerType)
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

val UnitCitation = TableSpec( 
  explodes = Seq( 
    "CrashReport.UnitLevel.Unit", // row0 = each unit 
    "UnitCitations.UnitCitation" // row1 = each unit citation 
    ), carry = Seq( 
      ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber", StringType), 
      ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType) 
      ), 
      cols = Seq( 
        //ColSpec("Id", "Id", IntegerType), 
        ColSpec("IssuedTo", "IssuedTo", StringType), 
        ColSpec("CitationNumber", "CitationNumber", StringType), 
        ColSpec("StatuteNumber", "StatuteNumber", StringType), 
        ColSpec("StatuteSeverityDesc", "StatuteSeverityDesc", StringType) 
   ) 
)

// Drug Types (from Person level)
val DrugTypes_DrugType = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",
    "Persons.Person",
    "AllPersons.DrugTest.DrugTypes.DrugType"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Person_Id","^row1.UnitPersonNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Endorse (License Endorsements from Person level)
val Endorse = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",
    "Persons.Person",
    "Driver.OperatingAsClassifiedEndorse.Endorse"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Person_Id","^row1.UnitPersonNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// HazMat Type (from Vehicle level)
val HazMatType = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",              // row0 = each unit
    "Vehicle",                                  // row1 = vehicle
    "HazMat.HazMatList.HazMatType"                   // row2 = each hazmat
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber", StringType),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Vehicle_Id", "^row0.UnitNumber", StringType)
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("PlacardDisplayed", "PlacardDisplayed.Code"),
    ColSpec("IDNumber", "IDNumber", IntegerType),
    ColSpec("ClassNumber", "ClassNumber", StringType),
    ColSpec("NameFromPlacard", "NameFromPlacard", StringType),
    ColSpec("CargoReleased", "CargoReleased.Code")
  )
)

// License Restriction (from Person level)
val LicenseRestriction = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",
    "Persons.Person",
    "AllPersons.DriverLicense.LicenseRestrictions.LicenseRestriction"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Person_Id","^row1.UnitPersonNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Property Owner
val PropertyOwner = TableSpec(
  explodes = Seq("CrashReport.PropertyOwners.PropertyOwner"),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber")
  ),
  cols = Seq(
    //ColSpec("PropertyOwner_Id", "PropertyOwnerNumber"),
    ColSpec("OwnerType", "OwnerDetails.OwnerType.Code"),
    ColSpec("OwnerNumber", "OwnerDetails.OwnerNumber"),
    ColSpec("DMVCustNumber", "DMVCustNumber"),
    ColSpec("CompanyGovtName", "OwnerDetails.CompanyGovtName"),
    ColSpec("OwnerName_LastName", "OwnerDetails.OwnerName.LastName"),
    ColSpec("OwnerName_FirstName", "OwnerDetails.OwnerName.FirstName"),
    ColSpec("OwnerName_MiddleName", "OwnerDetails.OwnerName.MiddleName"),
    ColSpec("OwnerName_Suffix", "OwnerDetails.OwnerName.Suffix"),
    ColSpec("OwnerAddress_AddressLine1", "OwnerDetails.OwnerAddress.AddressLine1"),
    ColSpec("OwnerAddress_City", "OwnerDetails.OwnerAddress.City"),
    ColSpec("OwnerAddress_State", "OwnerDetails.OwnerAddress.State"),
    ColSpec("OwnerAddress_Zipcode", "OwnerDetails.OwnerAddress.Zipcode"),
    ColSpec("OwnerPhone_PhoneNumber", "OwnerDetails.OwnerPhone.PhoneNumber")
  )
)

// Protective Gear (from Person level)
val ProtectiveGear = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",
    "Persons.Person",
    "Motorcycle.GearUsed.ProtectiveGear"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber"),
    ColSpec("Person_Id","^row1.UnitPersonNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Related Factor (from Fatal Supplement)
val RelatedFactor = TableSpec(
  explodes = Seq(
    "FatalSupplement.Units.Unit",
    "RelatedFactors.RelatedFactor"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("FatalSupplement_Unit_Id", "^row0.UnitNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Roadway Factor
val RoadwayFactor = TableSpec(
  explodes = Seq("CrashReport.CrashLevel.RoadwayFactors.RoadwayFactor"),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Roadway
val Roadway = TableSpec(explodes = Seq.empty, carry = Seq(
  ColSpec("Crash_Id", "CrashReport.Header.CrashNumber")
), cols = Seq(
  ColSpec("TLTFlag", "CrashReport.Roadway.LRS.TLT.TLTFlag", BooleanType),
  ColSpec("WISLRLinkID", "CrashReport.Roadway.LRS.TLT.WISLRLinkID"),
  ColSpec("WISLRLinkOset", "CrashReport.Roadway.LRS.TLT.WISLRLinkOset"),
  ColSpec("WISLRCoordinate_XCoordinate", "CrashReport.Roadway.LRS.TLT.WISLRCoordinate.XCoordinate"),
  ColSpec("WISLRCoordinate_YCoordinate", "CrashReport.Roadway.LRS.TLT.WISLRCoordinate.YCoordinate"),
  ColSpec("PopulationClass", "CrashReport.Roadway.PopulationClass.Code"),
  ColSpec("UrbanRuralDesg", "CrashReport.Roadway.UrbanRuralDesg.Code"),
  ColSpec("HwyClass", "CrashReport.Roadway.HwyClass.Code"),
  ColSpec("TLTSnapFlag", "CrashReport.Roadway.LRS.TLT.TLTSnapFlag", BooleanType)
))

// Safety Equipment Equipment (from Person level)
val SafetyEquipment_Equipment = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",
    "Persons.Person",
    "NonMotorist.SafetyEquipment.Equipment"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber"),
    ColSpec("Person_Id","^row1.UnitPersonNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Study Response
val StudyResponse = TableSpec(
  explodes = Seq("CrashReport.CrashLevel.SpecialStudy"),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("StudyResponseCode", "StudyResponse")
  )
)

// Trailer (from Vehicle level)
val Trailer = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",              // row0 = each unit
    "Vehicle",                                  // row1 = vehicle
    "Trailers.TrailerList.Trailer"                         // row2 = each trailer
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber", StringType),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Vehicle_Id", "^row0.UnitNumber", StringType)
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("TowedUnitType", "TowedUnitType.Code"),
    ColSpec("TrailerVIN", "TrailerVIN", StringType),
    ColSpec("TrailerLicensePlate_PlateNumber", "TrailerLicensePlate.PlateNumber", StringType),
    ColSpec("TrailerLicensePlate_PlateType", "TrailerLicensePlate.PlateType", StringType),
    ColSpec("TrailerLicensePlate_State", "TrailerLicensePlate.State", StringType),
    ColSpec("TrailerLicensePlate_Country", "TrailerLicensePlate.Country", StringType),
    ColSpec("TrailerMake", "TrailerMake", StringType),
    ColSpec("OwnerType", "OwnerDetails.OwnerType.Code"),
    ColSpec("OwnerNumber", "OwnerDetails.OwnerNumber", StringType),
    ColSpec("DMVCustNumber", "OwnerDetails.DMVCustNumber", StringType),
    ColSpec("CompanyGovtName", "OwnerDetails.CompanyGovtName", StringType),
    ColSpec("OwnerName_LastName", "OwnerDetails.OwnerName.LastName", StringType),
    ColSpec("OwnerName_FirstName", "OwnerDetails.OwnerName.FirstName", StringType),
    ColSpec("OwnerName_MiddleName", "OwnerDetails.OwnerName.MiddleName", StringType),
    ColSpec("OwnerName_Suffix", "OwnerDetails.OwnerName.Suffix", StringType),
    ColSpec("OwnerAddress_AddressLine1", "OwnerDetails.OwnerAddress.AddressLine1", StringType),
    ColSpec("OwnerAddress_AddressLine2", "OwnerDetails.OwnerAddress.AddressLine2", StringType),
    ColSpec("OwnerAddress_City", "OwnerDetails.OwnerAddress.City", StringType),
    ColSpec("OwnerAddress_State", "OwnerDetails.OwnerAddress.State", StringType),
    ColSpec("OwnerAddress_Zipcode", "OwnerDetails.OwnerAddress.Zipcode", StringType),
    ColSpec("OwnerAddress_POBox", "OwnerDetails.OwnerAddress.POBox", StringType),
    ColSpec("OwnerAddress_Country", "OwnerDetails.OwnerAddress.Country", StringType),
    ColSpec("OwnerPhone_PhoneNumber", "OwnerDetails.OwnerPhone.PhoneNumber", StringType),
    ColSpec("OwnerPhone_Extension", "OwnerDetails.OwnerPhone.Extension", StringType)
  )
)

// Truck Bus (from Vehicle level)
val TruckBus = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",              // row0 = each unit
    "Vehicle"                                   // row1 = vehicle
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber", StringType),
    ColSpec("Unit_Id", "^row0.UnitNumber", IntegerType),
    ColSpec("Vehicle_Id", "^row0.UnitNumber")
  ),
  cols = Seq(
    ColSpec("CarrierType", "TruckBus.CarrierInfo.CarrierType.Code"),
    ColSpec("CarrierNumSource", "TruckBus.CarrierInfo.CarrierNumSource.Code", StringType),
    ColSpec("CarrierIDNum", "TruckBus.CarrierInfo.CarrierIDNum", StringType),
    ColSpec("CarrierName", "TruckBus.CarrierInfo.CarrierName", StringType),
    ColSpec("CarrierAddress_AddressLine1", "TruckBus.CarrierInfo.CarrierAddress.AddressLine1", StringType),
    ColSpec("CarrierAddress_AddressLine2", "TruckBus.CarrierInfo.CarrierAddress.AddressLine2", StringType),
    ColSpec("CarrierAddress_City", "TruckBus.CarrierInfo.CarrierAddress.City", StringType),
    ColSpec("CarrierAddress_State", "TruckBus.CarrierInfo.CarrierAddress.State", StringType),
    ColSpec("CarrierAddress_Zipcode", "TruckBus.CarrierInfo.CarrierAddress.Zipcode", StringType),
    ColSpec("CarrierAddress_POBox", "TruckBus.CarrierInfo.CarrierAddress.POBox", StringType),
    ColSpec("CarrierAddress_Country", "TruckBus.CarrierInfo.CarrierAddress.Country", StringType),
    ColSpec("CarrierSameAsOwner", "TruckBus.CarrierInfo.CarrierSameAsOwner", BooleanType),
    ColSpec("VehicleConfig", "TruckBus.VehicleConfig.Code"),
    ColSpec("CargoBodyType", "TruckBus.CargoBodyType.Code"),
    ColSpec("PermittedLoadInfo_PermittedLoad", "TruckBus.PermittedLoadInfo.PermittedLoad.Code"),
    ColSpec("PermittedLoadInfo_PermitNumber", "TruckBus.PermittedLoadInfo.PermitNumber", StringType),
    ColSpec("PermittedLoadInfo_PermittedRoute", "TruckBus.PermittedLoadInfo.PermittedRoute", BooleanType),
    ColSpec("PermittedLoadInfo_EscortRequired", "TruckBus.PermittedLoadInfo.EscortRequired", BooleanType),
    ColSpec("PermittedLoadInfo_EscortPresent", "TruckBus.PermittedLoadInfo.EscortPresent", BooleanType),
    ColSpec("PermittedLoadInfo_OSOWLoadFlag", "TruckBus.PermittedLoadInfo.OSOWLoadFlag", BooleanType),
    ColSpec("MeasuredDimensions_MeasuredHeightFeet", "TruckBus.MeasuredDimensions.MeasuredHeightFeet", IntegerType),
    ColSpec("MeasuredDimensions_MeasureHeightInches", "TruckBus.MeasuredDimensions.MeasureHeightInches", IntegerType),
    ColSpec("MeasuredDimensions_MeasuredLengthFeet", "TruckBus.MeasuredDimensions.MeasuredLengthFeet", IntegerType),
    ColSpec("MeasuredDimensions_MeasuredLengthInches", "TruckBus.MeasuredDimensions.MeasuredLengthInches", IntegerType),
    ColSpec("MeasuredDimensions_MeasuredWidthFeet", "TruckBus.MeasuredDimensions.MeasuredWidthFeet", IntegerType),
    ColSpec("MeasuredDimensions_MeasuredWidthInches", "TruckBus.MeasuredDimensions.MeasuredWidthInches", IntegerType),
    ColSpec("MeasuredDimensions_MeasuredWeightLbs", "TruckBus.MeasuredDimensions.MeasuredWeightLbs", IntegerType),
    ColSpec("GrossVehWeightRating", "TruckBus.GrossVehWeightRating.Code"),
    ColSpec("TruckBusHazmat", "TruckBus.TruckBusHazmat.Code"),
    ColSpec("FMCSA_LastModified", "TruckBus.FMCSA.LastModified", StringType),
    ColSpec("FMCSA_TruckBusHazmat", "TruckBus.FMCSA.TruckBusHazmat.Code"),
    ColSpec("FMCSA_ReportingThreshold", "TruckBus.FMCSA.ReportingThreshold.Code")
  )
)


// Vehicle Circumstance (from Fatal Supplement)
val VehicleCircumstance = TableSpec(
  explodes = Seq(
    "FatalSupplement.Units.Unit",
    "VehicleCircumstances.VehicleCircumstance"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("FatalSupplement_Unit_Id", "^row0.UnitNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Vehicle Factor (from Vehicle level)
val VehicleFactor = TableSpec(
  explodes = Seq(
    "CrashReport.UnitLevel.Unit",
    "Vehicle.VehicleFactors.VehicleFactor"
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber"),
    ColSpec("Unit_Id", "^row0.UnitNumber"),
    ColSpec("Vehicle_Id", "^row0.UnitNumber")
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("Code", "Code")
  )
)

// Witness
val Witness = TableSpec(
  explodes = Seq(
    "CrashReport.CrashLevel.Witnesses.WitnessList.Witness"    // row0 = each witness
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber", StringType)
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("WitnessNumber", "WitnessNumber", StringType),
    ColSpec("WitnessName_LastName", "WitnessName.LastName", StringType),
    ColSpec("WitnessName_FirstName", "WitnessName.FirstName", StringType),
    ColSpec("WitnessName_MiddleName", "WitnessName.MiddleName", StringType),
    ColSpec("WitnessName_Suffix", "WitnessName.Suffix", StringType),
    ColSpec("WitnessAddress_AddressLine1", "WitnessAddress.AddressLine1", StringType),
    ColSpec("WitnessAddress_AddressLine2", "WitnessAddress.AddressLine2", StringType),
    ColSpec("WitnessAddress_City", "WitnessAddress.City", StringType),
    ColSpec("WitnessAddress_State", "WitnessAddress.State", StringType),
    ColSpec("WitnessAddress_Zipcode", "WitnessAddress.Zipcode", StringType),
    ColSpec("WitnessAddress_POBox", "WitnessAddress.POBox", StringType),
    ColSpec("WitnessAddress_Country", "WitnessAddress.Country", StringType),
    ColSpec("WitnessPhone_PhoneNumber", "WitnessPhone.PhoneNumber", StringType),
    ColSpec("WitnessPhone_Extension", "WitnessPhone.Extension", StringType),
    ColSpec("WitnessDOB", "WitnessDOB", StringType)
  )
)

// ObjectStruck
val ObjectStruck = TableSpec(
  explodes = Seq(
    "CrashReport","PropertyOwners.PropertyOwner","ObjectsStruck"    // row0
  ),
  carry = Seq(
    ColSpec("Crash_Id", "^CrashReport.Header.CrashNumber", StringType)
  ),
  cols = Seq(
    //ColSpec("Id", "Id", IntegerType),
    ColSpec("ObjectType", "ObjectStruck.ObjectType.Code", StringType),
    ColSpec("ObjectNumber", "ObjectStruck.ObjectNumber", StringType),
    ColSpec("GovtPropFlag", "ObjectStruck.GovtPropFlag", BooleanType), //
    ColSpec("GovtTagNum", "ObjectStruck.GovtTagNum", StringType),
    ColSpec("StruckByUnit", "ObjectStruck.StruckByUnit", StringType),
    ColSpec("AssetNumber", "ObjectStruck.AssetNumber", StringType)
  )
)

val tablesSpec: Map[String, TableSpec] = Map(
  "ActionTimeOfCrash_Action" -> ActionTimeOfCrash_Action,
  "Additional" -> Additional,
  "AnimalType" -> AnimalType,
  "Citation" -> Citation,
  "ClosureReason" -> ClosureReason,
  "CrashDiagram" -> CrashDiagram,
  "CrashLocation" -> CrashLocation,
  "CrashReport" -> CrashReport,
  "CrashReportImage" -> CrashReportImage,
  "EnvironmentalFactor" -> EnvironmentalFactor,
  "Flags" -> Flags,
  "Unit" -> UnitTbl,
  "Vehicle" -> Vehicle,
  "VehicleOwner" -> VehicleOwner,
  "Person" -> Person,
  "WeatherCondition" -> WeatherCondition,
  "RoadSurfaceCondition" -> RoadSurfaceCondition,
  "LicenseRestriction" -> LicenseRestriction,
  "DriverAction" -> DriverAction,
  "DriverManeuveredtoAvoidCode" -> DriverManeuveredtoAvoidCode,
  "DriverPedCondition" -> DriverPedCondition,
  "DrugTypes_DrugType" -> DrugTypes_DrugType,
  "Endorse" -> Endorse,
  "FatalSupplement" -> FatalSupplement,
  "FatalSupplement_FatalReportImage" -> FatalSupplement_FatalReportImage,
  "FatalSupplement_Person" -> FatalSupplement_Person,
  "FatalSupplement_Unit" -> FatalSupplement_Unit,
  "HazMatType" -> HazMatType,
  "DamageArea" -> DamageArea,
  "LicenseRestriction" -> LicenseRestriction,
  "ProtectiveGear" -> ProtectiveGear,
  "RelatedFactor" -> RelatedFactor,
  "RoadSurfaceCondition" -> RoadSurfaceCondition,
  "Roadway" -> Roadway,
  "RoadwayFactor" -> RoadwayFactor,
  "SafetyEquipment_Equipment" -> SafetyEquipment_Equipment,
  "StudyResponse" -> StudyResponse,
  "Trailer" -> Trailer,
  "TruckBus" -> TruckBus,
  "Witness" -> Witness,
  "WeatherCondition" -> WeatherCondition,
  //"DistractedBy" -> DistractedBy,
  "DriversVisionObscuredByCode" -> DriversVisionObscuredByCode,
  "UnitCitation" -> UnitCitation,
  "VehicleCircumstance" -> VehicleCircumstance,
  "VehicleFactor" -> VehicleFactor,
  "PropertyOwner" -> PropertyOwner,
  "ObjectStruck" -> ObjectStruck
)


// Build all tables using the helper functions
val tables_init: Map[String, DataFrame] = tablesSpec.map { case (name, spec) =>
  val df = makeTable(rootDF, spec)
  df.createOrReplaceTempView(s"vw_${name}")
  name -> df
}.toMap

//display(tables_init("FatalSupplement"))
