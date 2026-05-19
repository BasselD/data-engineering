No problem. Start clean.

## Assumption

For the SQL below, **`LastFillAdjusted` must be the adjusted medication coverage end date / exhaust date**, not just the shifted fill start date.

If `LastFillAdjusted` is only the shifted fill date, you still need `DaySupply`.

---

## Simple SQL

```sql
WITH base AS (
    SELECT
        MemberID,
        MedicationClass,
        LastUpdate,
        Numerator,
        Denominator,
        LastFillAdjusted,

        DATE '2026-12-31' AS YearEndDate,

        -- Current denominator + remaining calendar days in year
        Denominator + (DATE '2026-12-31' - LastUpdate) AS ProjectedDenominatorEOY,

        -- Future covered days already on hand after report date
        CASE
            WHEN LastFillAdjusted > LastUpdate
                THEN LastFillAdjusted - LastUpdate
            ELSE 0
        END AS FutureCoveredDaysOnHand

    FROM your_table
),

calc AS (
    SELECT
        *,
        Numerator + FutureCoveredDaysOnHand AS ProjectedNumerator,

        CEILING(ProjectedDenominatorEOY * 0.80) AS RequiredCoveredDaysEOY
    FROM base
)

SELECT
    MemberID,
    MedicationClass,
    LastUpdate,
    Numerator,
    Denominator,
    LastFillAdjusted,
    ProjectedNumerator,
    ProjectedDenominatorEOY,
    RequiredCoveredDaysEOY,

    RequiredCoveredDaysEOY - ProjectedNumerator AS RemainingCoveredDaysNeeded,

    CASE
        WHEN RequiredCoveredDaysEOY - ProjectedNumerator <= 0
            THEN 'Already Safe'

        WHEN RequiredCoveredDaysEOY - ProjectedNumerator > (YearEndDate - LastFillAdjusted)
            THEN 'Non-Recoverable'

        ELSE 'Recoverable'
    END AS RecoverabilityStatus,

    CASE
        WHEN RequiredCoveredDaysEOY - ProjectedNumerator <= 0
            THEN NULL

        WHEN RequiredCoveredDaysEOY - ProjectedNumerator > (YearEndDate - LastFillAdjusted)
            THEN NULL

        ELSE YearEndDate - (RequiredCoveredDaysEOY - ProjectedNumerator) + 1
    END AS LastImpactDate

FROM calc;
```

---

## Important Correction

If this field:

```sql
LastFillAdjusted = DATE '2026-05-26'
```

means **adjusted fill start date**, then the SQL above is incomplete.

You need:

```text
AdjustedFillStartDate + DaySupply - 1 = AdjustedMedicationExhaustDate
```

Example:

| Event                              |       Date |
| ---------------------------------- | ---------: |
| First prescription start           | 02/16/2026 |
| Second prescription actual fill    | 05/01/2026 |
| Second prescription adjusted start | 05/26/2026 |
| Day supply                         |         30 |
| Adjusted medication exhaust date   | 06/24/2026 |

So the field you really want in the SQL is:

```sql
AdjustedMedicationExhaustDate = DATE '2026-06-24'
```

Not May 26.

## Bottom Line

* If `LastFillAdjusted` = **adjusted coverage end date**, use it directly.
* If `LastFillAdjusted` = **adjusted fill start date**, add `DaySupply - 1` first.
* Last Impact Date should be based on the **adjusted exhaust date**, not the raw fill date.
