WITH full_year_target AS (
    SELECT 
        patient_id,
        ReportAnchorDate,
        Numerator AS ytd_numerator,
        Denominator AS ytd_denominator,
        FirstFillDate,
        LastFillDate,
        LastDaysSupply,
        -- 1. Project the full year denominator (First Fill to Dec 31)
        DATE_DIFF('2026-12-31', FirstFillDate, DAY) + 1 AS full_year_denominator,
        -- 2. Calculate absolute target days needed for 80% Stars adherence
        CEIL(0.80 * (DATE_DIFF('2026-12-31', FirstFillDate, DAY) + 1)) AS req_covered_days
    FROM your_adherence_ytd_table
),

adherence_deficit AS (
    SELECT 
        *,
        -- 3. Determine remaining days needed to hit the 80% threshold
        GREATEST(0, req_covered_days - ytd_numerator) AS days_needed,
        
        -- 4. Account for medication still in hand past the anchor date.
        -- NOTE: If your PDC pipeline outputs an 'Adjusted_End_Date' for the last fill, use that instead.
        GREATEST(0, DATE_DIFF(DATE_ADD(LastFillDate, INTERVAL LastDaysSupply DAY), ReportAnchorDate, DAY)) AS days_supply_on_hand,
        
        -- Total calendar days left in the year from the anchor date
        DATE_DIFF('2026-12-31', ReportAnchorDate, DAY) AS calendar_days_remaining
    FROM full_year_target
),

timeline_calculations AS (
    SELECT 
        *,
        -- 5. Back into the last impact date from December 31st
        DATE_SUB('2026-12-31', INTERVAL (days_needed - days_supply_on_hand - 1) DAY) AS calculated_last_impact_date
    FROM adherence_deficit
)

SELECT 
    patient_id,
    ReportAnchorDate,
    ytd_numerator,
    req_covered_days,
    days_needed,
    days_supply_on_hand,
    -- 6. Apply business guardrails for reporting
    CASE 
        -- Case A: Patient has already locked in 80% adherence for the year
        WHEN days_needed = 0 THEN '2026-12-31' 
        
        -- Case B: Days supply on hand already satisfies the remaining deficit
        WHEN days_supply_on_hand >= days_needed THEN '2026-12-31'
        
        -- Case C: Mathematically impossible to reach 80% even if filled today
        WHEN (days_needed - days_supply_on_hand) > calendar_days_remaining THEN NULL 
        
        -- Case D: Valid actionable Last Impact Date
        ELSE calculated_last_impact_date
    END AS last_impact_date,
    
    CASE 
        WHEN days_needed = 0 OR days_supply_on_hand >= days_needed THEN 'Adherence Achieved'
        WHEN (days_needed - days_supply_on_hand) > calendar_days_remaining THEN 'Opportunity Missed'
        ELSE 'Actionable'
    END AS adherence_status
FROM timeline_calculations;
