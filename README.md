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

# Task Two
1. Take list of funds and fetch performance ids
2. Obtain shareclassid from get-detail
3. scrape from morningstar depending on the data from get-detail
4. return a csv result

* Bad Symbols:
* VMRXX
* FACAX
* FBBHX
* FPFAX
* UNTCW
* GOFXX
* AFAXX
* BRK/B
* BITW
* LTCN