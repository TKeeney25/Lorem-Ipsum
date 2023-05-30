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

# Refactor
## Issues
* [ ] database.py return values (should be moved to a different logic layer)
* [ ] settings month updates regardless of if the screen is running or something else.
* [ ] Emailing errors should be an optional thing and not required. 
* [ ] Emailing results should be an optional argument
* [ ] last_month_ran should be moved away from settings.json
* [ ] An intuitive naming schema should be set up for the fund screener as well as the fund fetcher. Too similar otherwise.
* [ ] ^ Same thing for the files utilized by both
* [ ] master_thread should become a class
* [ ] main.py should be split into smaller files
* [ ] ensure everything has a type hint

## Features
* [ ] Auto-update
* [ ] Progress bar
* [ ] Enhanced error catching and reporting
* [ ] 