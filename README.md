Handle data missing issues in Rates EOD Limit Check reports

Capacity & Performance - Operational Efficiency

Acceptance Criteria:- 

Scenario: Rates EOD Limit Check report should be generated
Given: The data for any Volcker Trading desk for Rates is not available
When: Rates EOD Limit check job is run from Bob Monitor
Then:
1. EOD Limit based check report should be generated for the VTDs that have data
2. A failure alert should be generated for the VTDs that do not have data
