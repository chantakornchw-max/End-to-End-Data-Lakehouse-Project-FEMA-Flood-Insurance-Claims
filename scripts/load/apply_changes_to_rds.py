import os
import csv
import sys
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def apply_changes(csv_file):
    
    # Connect to RDS
    conn = psycopg2.connect(
        host=os.getenv("RDS_HOST"),
        port=5432,
        database=os.getenv("DB_NAME"),
        user=os.getenv("RDS_USER"),
        password=os.getenv("RDS_PASSWORD")
    )

    cur = conn.cursor()

    try:
        # Read change file
        with open(csv_file, "r", encoding="utf-8-sig") as file:
            reader = csv.DictReader(file)

            for row in reader:

                operation = row["operation"]
                record_id = row["id"]

                # --------------------------------
                # INSERT
                # --------------------------------
                if operation == "INSERT":

                    cur.execute(
                        """
                        INSERT INTO public.raw_fema_claims (
                            agricultureStructureIndicator,
                            asOfDate,
                            basementEnclosureCrawlspaceType,
                            policyCount,
                            crsClassificationCode,
                            dateOfLoss,
                            elevatedBuildingIndicator,
                            elevationCertificateIndicator,
                            elevationDifference,
                            baseFloodElevation,
                            ratedFloodZone,
                            houseWorship,
                            locationOfContents,
                            lowestAdjacentGrade,
                            lowestFloorElevation,
                            numberOfFloorsInTheInsuredBuilding,
                            nonProfitIndicator,
                            obstructionType,
                            occupancyType,
                            originalConstructionDate,
                            originalNBDate,
                            amountPaidOnBuildingClaim,
                            amountPaidOnContentsClaim,
                            amountPaidOnIncreasedCostOfComplianceClaim,
                            postFIRMConstructionIndicator,
                            rateMethod,
                            smallBusinessIndicatorBuilding,
                            totalBuildingInsuranceCoverage,
                            totalContentsInsuranceCoverage,
                            yearOfLoss,
                            primaryResidenceIndicator,
                            buildingDamageAmount,
                            buildingDeductibleCode,
                            netBuildingPaymentAmount,
                            buildingPropertyValue,
                            causeOfDamage,
                            condominiumCoverageTypeCode,
                            contentsDamageAmount,
                            contentsDeductibleCode,
                            netContentsPaymentAmount,
                            contentsPropertyValue,
                            disasterAssistanceCoverageRequired,
                            eventDesignationNumber,
                            ficoNumber,
                            floodCharacteristicsIndicator,
                            floodWaterDuration,
                            floodproofedIndicator,
                            floodEvent,
                            iccCoverage,
                            netIccPaymentAmount,
                            nfipRatedCommunityNumber,
                            nfipCommunityNumberCurrent,
                            nfipCommunityName,
                            nonPaymentReasonContents,
                            nonPaymentReasonBuilding,
                            numberOfUnits,
                            buildingReplacementCost,
                            contentsReplacementCost,
                            replacementCostBasis,
                            stateOwnedIndicator,
                            waterDepth,
                            floodZoneCurrent,
                            buildingDescriptionCode,
                            rentalPropertyIndicator,
                            state,
                            reportedCity,
                            reportedZipCode,
                            countyCode,
                            censusTract,
                            censusBlockGroupFips,
                            latitude,
                            longitude,
                            id,
                            source_updated_at
                        )
                        VALUES (
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s,
                            %s, %s, %s, %s
                        )
                        """,
                        (
                            row["agricultureStructureIndicator"],
                            row["asOfDate"],
                            row["basementEnclosureCrawlspaceType"],
                            row["policyCount"],
                            row["crsClassificationCode"],
                            row["dateOfLoss"],
                            row["elevatedBuildingIndicator"],
                            row["elevationCertificateIndicator"],
                            row["elevationDifference"],
                            row["baseFloodElevation"],
                            row["ratedFloodZone"],
                            row["houseWorship"],
                            row["locationOfContents"],
                            row["lowestAdjacentGrade"],
                            row["lowestFloorElevation"],
                            row["numberOfFloorsInTheInsuredBuilding"],
                            row["nonProfitIndicator"],
                            row["obstructionType"],
                            row["occupancyType"],
                            row["originalConstructionDate"],
                            row["originalNBDate"],
                            row["amountPaidOnBuildingClaim"],
                            row["amountPaidOnContentsClaim"],
                            row["amountPaidOnIncreasedCostOfComplianceClaim"],
                            row["postFIRMConstructionIndicator"],
                            row["rateMethod"],
                            row["smallBusinessIndicatorBuilding"],
                            row["totalBuildingInsuranceCoverage"],
                            row["totalContentsInsuranceCoverage"],
                            row["yearOfLoss"],
                            row["primaryResidenceIndicator"],
                            row["buildingDamageAmount"],
                            row["buildingDeductibleCode"],
                            row["netBuildingPaymentAmount"],
                            row["buildingPropertyValue"],
                            row["causeOfDamage"],
                            row["condominiumCoverageTypeCode"],
                            row["contentsDamageAmount"],
                            row["contentsDeductibleCode"],
                            row["netContentsPaymentAmount"],
                            row["contentsPropertyValue"],
                            row["disasterAssistanceCoverageRequired"],
                            row["eventDesignationNumber"],
                            row["ficoNumber"],
                            row["floodCharacteristicsIndicator"],
                            row["floodWaterDuration"],
                            row["floodproofedIndicator"],
                            row["floodEvent"],
                            row["iccCoverage"],
                            row["netIccPaymentAmount"],
                            row["nfipRatedCommunityNumber"],
                            row["nfipCommunityNumberCurrent"],
                            row["nfipCommunityName"],
                            row["nonPaymentReasonContents"],
                            row["nonPaymentReasonBuilding"],
                            row["numberOfUnits"],
                            row["buildingReplacementCost"],
                            row["contentsReplacementCost"],
                            row["replacementCostBasis"],
                            row["stateOwnedIndicator"],
                            row["waterDepth"],
                            row["floodZoneCurrent"],
                            row["buildingDescriptionCode"],
                            row["rentalPropertyIndicator"],
                            row["state"],
                            row["reportedCity"],
                            row["reportedZipCode"],
                            row["countyCode"],
                            row["censusTract"],
                            row["censusBlockGroupFips"],
                            row["latitude"],
                            row["longitude"],
                            row["id"],
                            row["source_updated_at"]
                        )
                    )

                # --------------------------------
                # UPDATE
                # --------------------------------
                elif operation == "UPDATE":

                    cur.execute(
                        """
                        UPDATE public.raw_fema_claims
                        SET
                            agricultureStructureIndicator = %s,
                            asOfDate = %s,
                            basementEnclosureCrawlspaceType = %s,
                            policyCount = %s,
                            crsClassificationCode = %s,
                            dateOfLoss = %s,
                            elevatedBuildingIndicator = %s,
                            elevationCertificateIndicator = %s,
                            elevationDifference = %s,
                            baseFloodElevation = %s,
                            ratedFloodZone = %s,
                            houseWorship = %s,
                            locationOfContents = %s,
                            lowestAdjacentGrade = %s,
                            lowestFloorElevation = %s,
                            numberOfFloorsInTheInsuredBuilding = %s,
                            nonProfitIndicator = %s,
                            obstructionType = %s,
                            occupancyType = %s,
                            originalConstructionDate = %s,
                            originalNBDate = %s,
                            amountPaidOnBuildingClaim = %s,
                            amountPaidOnContentsClaim = %s,
                            amountPaidOnIncreasedCostOfComplianceClaim = %s,
                            postFIRMConstructionIndicator = %s,
                            rateMethod = %s,
                            smallBusinessIndicatorBuilding = %s,
                            totalBuildingInsuranceCoverage = %s,
                            totalContentsInsuranceCoverage = %s,
                            yearOfLoss = %s,
                            primaryResidenceIndicator = %s,
                            buildingDamageAmount = %s,
                            buildingDeductibleCode = %s,
                            netBuildingPaymentAmount = %s,
                            buildingPropertyValue = %s,
                            causeOfDamage = %s,
                            condominiumCoverageTypeCode = %s,
                            contentsDamageAmount = %s,
                            contentsDeductibleCode = %s,
                            netContentsPaymentAmount = %s,
                            contentsPropertyValue = %s,
                            disasterAssistanceCoverageRequired = %s,
                            eventDesignationNumber = %s,
                            ficoNumber = %s,
                            floodCharacteristicsIndicator = %s,
                            floodWaterDuration = %s,
                            floodproofedIndicator = %s,
                            floodEvent = %s,
                            iccCoverage = %s,
                            netIccPaymentAmount = %s,
                            nfipRatedCommunityNumber = %s,
                            nfipCommunityNumberCurrent = %s,
                            nfipCommunityName = %s,
                            nonPaymentReasonContents = %s,
                            nonPaymentReasonBuilding = %s,
                            numberOfUnits = %s,
                            buildingReplacementCost = %s,
                            contentsReplacementCost = %s,
                            replacementCostBasis = %s,
                            stateOwnedIndicator = %s,
                            waterDepth = %s,
                            floodZoneCurrent = %s,
                            buildingDescriptionCode = %s,
                            rentalPropertyIndicator = %s,
                            state = %s,
                            reportedCity = %s,
                            reportedZipCode = %s,
                            countyCode = %s,
                            censusTract = %s,
                            censusBlockGroupFips = %s,
                            latitude = %s,
                            longitude = %s,
                            source_updated_at = %s
                        WHERE id = %s
                        """,
                        (
                            row["agricultureStructureIndicator"],
                            row["asOfDate"],
                            row["basementEnclosureCrawlspaceType"],
                            row["policyCount"],
                            row["crsClassificationCode"],
                            row["dateOfLoss"],
                            row["elevatedBuildingIndicator"],
                            row["elevationCertificateIndicator"],
                            row["elevationDifference"],
                            row["baseFloodElevation"],
                            row["ratedFloodZone"],
                            row["houseWorship"],
                            row["locationOfContents"],
                            row["lowestAdjacentGrade"],
                            row["lowestFloorElevation"],
                            row["numberOfFloorsInTheInsuredBuilding"],
                            row["nonProfitIndicator"],
                            row["obstructionType"],
                            row["occupancyType"],
                            row["originalConstructionDate"],
                            row["originalNBDate"],
                            row["amountPaidOnBuildingClaim"],
                            row["amountPaidOnContentsClaim"],
                            row["amountPaidOnIncreasedCostOfComplianceClaim"],
                            row["postFIRMConstructionIndicator"],
                            row["rateMethod"],
                            row["smallBusinessIndicatorBuilding"],
                            row["totalBuildingInsuranceCoverage"],
                            row["totalContentsInsuranceCoverage"],
                            row["yearOfLoss"],
                            row["primaryResidenceIndicator"],
                            row["buildingDamageAmount"],
                            row["buildingDeductibleCode"],
                            row["netBuildingPaymentAmount"],
                            row["buildingPropertyValue"],
                            row["causeOfDamage"],
                            row["condominiumCoverageTypeCode"],
                            row["contentsDamageAmount"],
                            row["contentsDeductibleCode"],
                            row["netContentsPaymentAmount"],
                            row["contentsPropertyValue"],
                            row["disasterAssistanceCoverageRequired"],
                            row["eventDesignationNumber"],
                            row["ficoNumber"],
                            row["floodCharacteristicsIndicator"],
                            row["floodWaterDuration"],
                            row["floodproofedIndicator"],
                            row["floodEvent"],
                            row["iccCoverage"],
                            row["netIccPaymentAmount"],
                            row["nfipRatedCommunityNumber"],
                            row["nfipCommunityNumberCurrent"],
                            row["nfipCommunityName"],
                            row["nonPaymentReasonContents"],
                            row["nonPaymentReasonBuilding"],
                            row["numberOfUnits"],
                            row["buildingReplacementCost"],
                            row["contentsReplacementCost"],
                            row["replacementCostBasis"],
                            row["stateOwnedIndicator"],
                            row["waterDepth"],
                            row["floodZoneCurrent"],
                            row["buildingDescriptionCode"],
                            row["rentalPropertyIndicator"],
                            row["state"],
                            row["reportedCity"],
                            row["reportedZipCode"],
                            row["countyCode"],
                            row["censusTract"],
                            row["censusBlockGroupFips"],
                            row["latitude"],
                            row["longitude"],
                            row["source_updated_at"],
                            record_id
                        )
                    )

                    if cur.rowcount != 1:
                            raise ValueError(f"UPDATE failed: id={record_id} was not found in RDS.")

                else:
                    raise ValueError(f"Unknown operation: {operation}")

        # Commit all changes
        conn.commit()
        print(f"Successfully applied: {csv_file}")

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        raise ValueError("CSV file path is required.")

    apply_changes(sys.argv[1])