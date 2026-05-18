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
