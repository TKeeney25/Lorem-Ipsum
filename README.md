Data Collection Phases

1. Screen (collects basic information)
2. Pre-Filter (Filters based off of screen information)
3. MS performance ID collect
3. YH Data Collect or MS Data Collect
4. Filter Data and Repeat #4
5. Output csv with data and flags.

Screen:
* Gives:
  * Type
  * Offset
  * Floor
  * Roof
* Needs:
  * Max Total Results
  * Current Total Results

ScreenStates:
* Mutual Fund
  * IF ready -> defaults
  * Elif etf -> continue
  * Elif mutual fund -> progress
* ETF
  * IF mutual fund -> defaults
  * Elif etf -> progress

Commonality
* If State == PCurrent -> Progress
* If State == Ready -> defaults
* If Current == ETF and State == Mutual Fund -> defaults
* If Current == Mutual Fund and State == ETF -> continue