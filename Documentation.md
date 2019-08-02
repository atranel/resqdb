## RES-Q database package
### June 18, 2019
Change in the ``Reports.py``:
1. Export into excel workbook was added. All created dataframes (thrombolysis, thrombectomy and stats per region) were added into this spreadsheet. Each month is in the single spreadsheet. 
2. Based on spreadsheet stats small bug in the calculation of incorrect times has been discovered. When the incorrect times were calculated, only times in minutes calculated from timestamps were taken into consideration but we forgot to calculated also times entered in minutes. 

### June 26, 2019
Regenerate documentation using ``sphinx`` package. 
1. Go to the folder docs in the package. 
2. Run command:
```bash
sphinx-apidoc -f -o source/ ../
make html
```
This command will regenerate documentation in the html format. 


