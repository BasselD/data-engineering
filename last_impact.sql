/*
/* Metrics & Adherence */

, PDC.AdherenceMeasure AS AdherenceMeasure
, PDC.NumUnadj AS CurrentNumerator
, PDC.DenUnadj AS CurrentDenominator

/* Current PDC */
, (1.00 * CurrentNumerator) / NULLIF(CurrentDenominator, 0) AS CurrentPDC

/* Key Dates */
, CAST(PDC.PDC_RunDate AS DATE) AS PDCCalculationDate

, CAST(CAST(YEAR(DATE) AS VARCHAR(4)) || '-12-31' AS DATE) AS YearEndDate

/* Treatment Span */
, CurrentDenominator AS TreatmentSpanDays

/* Current adherence math */
, CAST(FLOOR(TreatmentSpanDays * 0.80) AS INT) AS RequiredAdherenceDays

, CAST(FLOOR(TreatmentSpanDays * 0.20) AS INT) AS DaysCanBeMissed

, CAST(CurrentDenominator - CurrentNumerator AS INT) AS DaysAlreadyMissed

, GREATEST(DaysCanBeMissed - DaysAlreadyMissed, 0) AS DaysRemaining

/* Medication on hand */
/* LastFillDateAdjusted is assumed to be the overlap-adjusted fill start date */
, CAST(PDC.LastFillAdjusted AS DATE) AS LastFillDateAdjusted

, CAST(RNKR.LastDS AS INT) AS LastDaysSupply

, LastFillDateAdjusted + LastDaysSupply - 1 AS MedicationRunoutDate

/* Future covered days already available after the report anchor date */
, GREATEST(MedicationRunoutDate - PDCCalculationDate, 0) AS MedicationRunoutDays

/* Covered days after counting medication already on hand */
, CurrentNumerator + MedicationRunoutDays AS CoveredDaysWithRunout

/* Year-end denominator */
, CurrentDenominator + (YearEndDate - PDCCalculationDate) AS YearEndDenominator

/* Year-end PDC after counting current medication on hand */
, CAST(CoveredDaysWithRunout AS FLOAT) 
    / NULLIF(YearEndDenominator, 0) AS YearEndPDCWithRunout

/* Days still needed to hit 80% by year-end after counting current medication on hand */
, GREATEST(
      CEILING(YearEndDenominator * 0.80) - CoveredDaysWithRunout,
      0
  ) AS RequiredDaysForAdherence

/* Maximum possible adherence if perfectly covered from report date to year-end */
, CurrentNumerator + (YearEndDate - PDCCalculationDate) AS MaxPotentialNumerator

, YearEndDenominator AS MaxPotentialDenominator

, CAST(MaxPotentialNumerator AS FLOAT) 
    / NULLIF(MaxPotentialDenominator, 0) AS MaxPotentialPDC

/* Recoverability */
, CASE
      WHEN MaxPotentialPDC < 0.80 THEN 0
      ELSE 1
  END AS RecoverableFlag

/* Last Impact Date */
/* Latest date member must resume continuous coverage to still reach 80% by year-end */
, CASE
      WHEN RecoverableFlag = 0 THEN NULL
      WHEN RequiredDaysForAdherence = 0 THEN NULL
      ELSE YearEndDate - RequiredDaysForAdherence + 1
  END AS LastImpactDate

/* Days until last impact */
, CASE
      WHEN LastImpactDate IS NULL THEN NULL
      ELSE LastImpactDate - PDCCalculationDate
  END AS DaysUntilUnrecoverable
*/

WITH FullYearTarget AS (
    SELECT 
        MemberID,
        ReportAnchorDate,
        CurrentNumerator,
        CurrentDenominator,
        FirstFillDate,
        PdcAdjustedEndDate, -- The true end date of coverage after accounting for overlaps
        -- 1. Project the full year denominator (First Fill to Dec 31)
        DATE_DIFF('2026-12-31', FirstFillDate, DAY) + 1 AS FullYearDenominator,
        -- 2. Calculate absolute target days needed for 80% Stars adherence
        CEIL(0.80 * (DATE_DIFF('2026-12-31', FirstFillDate, DAY) + 1)) AS RequiredCoveredDays
    FROM YourAdherenceYtdTable
),

AdherenceDeficit AS (
    SELECT 
        *,
        -- 3. Determine remaining days needed to hit the 80% threshold
        GREATEST(0, RequiredCoveredDays - CurrentNumerator) AS DaysNeeded,
        
        -- 4. Calculate supply on hand using the pipeline's Adjusted End Date
        GREATEST(0, DATE_DIFF(PdcAdjustedEndDate, ReportAnchorDate, DAY)) AS DaysSupplyOnHand,
        
        -- Total calendar days left in the year from the anchor date
        DATE_DIFF('2026-12-31', ReportAnchorDate, DAY) AS CalendarDaysRemaining
    FROM FullYearTarget
),

TimelineCalculations AS (
    SELECT 
        *,
        -- 5. Back into the last impact date from December 31st
        DATE_SUB('2026-12-31', INTERVAL (DaysNeeded - DaysSupplyOnHand - 1) DAY) AS CalculatedLastImpactDate
    FROM AdherenceDeficit
)

SELECT 
    MemberID,
    ReportAnchorDate,
    CurrentNumerator,
    RequiredCoveredDays,
    DaysNeeded,
    DaysSupplyOnHand,
    -- 6. Apply business guardrails for reporting
    CASE 
        -- Case A: Patient has already locked in 80% adherence for the year
        WHEN DaysNeeded = 0 THEN '2026-12-31' 
        
        -- Case B: Days supply on hand already satisfies the remaining deficit
        WHEN DaysSupplyOnHand >= DaysNeeded THEN '2026-12-31'
        
        -- Case C: Mathematically impossible to reach 80% even if filled today
        WHEN (DaysNeeded - DaysSupplyOnHand) > CalendarDaysRemaining THEN NULL 
        
        -- Case D: Valid actionable Last Impact Date
        ELSE CalculatedLastImpactDate
    END AS LastImpactDate,
    
    CASE 
        WHEN DaysNeeded = 0 OR DaysSupplyOnHand >= DaysNeeded THEN 'Adherence Achieved'
        WHEN (DaysNeeded - DaysSupplyOnHand) > CalendarDaysRemaining THEN 'Opportunity Missed'
        ELSE 'Actionable'
    END AS AdherenceStatus
FROM TimelineCalculations;
