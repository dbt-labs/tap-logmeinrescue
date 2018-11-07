# tap-logmeinrescue

Author: Connor McArthur (connor@fishtownanalytics.com)

This is a [Singer](http://singer.io) tap that produces JSON-formatted data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

It:

- Generates a catalog of available data in LogMeIn Rescue
- Extracts the following resources:
  - [Technicians](https://secure.logmeinrescue.com/welcome/webhelp/EN/RescueAPI/API/API_Rescue_getHierarchy_v2.html) ([source](../master/tap_logmeinrescue/streams/technicians.py)): It pulls the entire list of technicians each time it runs.
  - [Session Report](https://secure.logmeinrescue.com/welcome/webhelp/EN/RescueAPI/API/API_Rescue_getReport_output_reports.html) ([source](../master/tap_logmeinrescue/streams/session_report.py)): It pulls all of the session report records, keeping track of where it left off each time. The reports below use the same pagination and state strategy.
  - [Technician Survey Report](https://secure.logmeinrescue.com/welcome/webhelp/EN/RescueAPI/API/API_Rescue_getReport_output_reports.html) ([source](../master/tap_logmeinrescue/streams/technician_survey_report.py))
  - [Transferred Sessions Extended Report](https://secure.logmeinrescue.com/welcome/webhelp/EN/RescueAPI/API/API_Rescue_getReport_output_reports.html) ([source](../master/tap_logmeinrescue/streams/transferred_sessions_extended_report.py))


### Quick Start

1. Install

```bash
git clone git@github.com:fishtown-analytics/tap-logmeinrescue.git
cd tap-logmeinrescue
pip install .
```

2. Create the config file.

There is a template you can use at `config.json.example`, just copy it to `config.json` in the repo root and insert your username and password.

4. Run the application to generate a catalog.

```bash
tap-logmeinrescue -c config.json --discover > catalog.json
```

5. Select the tables you'd like to replicate

Step 4 a file called `catalog.json` that specifies all the available endpoints and fields. You'll need to open the file and select the ones you'd like to replicate. See the [Singer guide on Catalog Format](https://github.com/singer-io/getting-started/blob/c3de2a10e10164689ddd6f24fee7289184682c1f/BEST_PRACTICES.md#catalog-format) for more information on how tables are selected.

6. Run it!

```bash
tap-logmeinrescue -c config.json --properties catalog.json
```

### Gotchas

- If you select any of the `*_report` streams, you MUST select `technician` as well.

---

Copyright &copy; 2018 Stitch
