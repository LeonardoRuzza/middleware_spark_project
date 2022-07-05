# Apache Spark Project for the course Middleware Technologies For Distributed Systems

The goal of the project is to analyze open data of the Covid-19 pandemic (3 queries in particular).

Input: csv files with report of Covid19, having the following schema.
(dateRep: String, day: Int, month: Int ,year: Int, cases: Int, deaths:Int, countriesAndTerritories: String, geoId: String, countryTerritoryCode: String, popData2019: Int, continentExp: String, Cumulative_number_for_14_days_of_COVID_19_cases_per_100000: Float)
 
 Queries:
  Q1. Seven days moving average of new reported cases, for each country and for each day.
  Q2. Percentage increase (with respect to the day before) of the seven days moving average, for each country and for each day.
  Q3. Top 10 countries with the highest percentage increase of the seven days moving average, for each day.
