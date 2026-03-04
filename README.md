# Analytics Engineering Case Study

This project contains the end-to-end data pipeline for the Analytics Engineer task - to measure driver engagement. It covers everything from initial Snowflake setup to final reporting models.

## Where to start
* **Appendix AE.pdf**: Start here for a detailed technical overview of the logic, data quality decisions, and architectural choices.
---

## 🏗 Project Structure

* **setup.sql**: The entry point for the project. Run this first to set up the Snowflake environment (databases, schemas, and stages).
* **models/**: Contains the core transformation logic divided into three layers:
    * **/staging**: Initial cleaning and data quality flagging (casting types, handling "ghost drivers").
    * **/marts**: Final dimensional models (Facts and Dimensions) ready for BI tools.
    * **/analyses**: Ad-hoc SQL queries and the Exploratory Data Analysis (EDA) notebook used to uncover trends.

---

## 🚀 How to use this repository
1. Run the `setup.sql` script in your Snowflake console.
2. Follow the loading order described in the Appendix: **Drivers -> Bookings -> Offers**.
3. The final aggregated metrics can be found in the `agg_driver_activity.sql` model within the **marts** folder.