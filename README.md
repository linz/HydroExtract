# HydroExtract
Extractor for LDS metadata - README

Grabs LDS metadata and parses it into CSV using XML paths as column headers.
Originally written in response to a hydrographic request but can be used for any LDS layer

## Operation

Reads Layer IDs from capabilities for WFS and WMS and uses these in metadata
URLs filtering on the keyword field (In this case "Hydrographic"). The metadata
document is parsed using XSLT into a python dict string and evaluated. The dict 
is read into a temporary in memory Sqlite database building a table for final 
output as CSV

## Notes
A simple import solution is to sym link added libs e.g.
```
ln -s ../LDS/LDSAPI/APIInterface/LDSAPI.py LDSAPI.py
ln -s ../lds_layer_checker/LinzUtil.py LinzUtil.py
ln -s ../LDS/LDSAPI/LXMLWrapper/LDSLXML.py LDSLXML.py
ln -s ../lds_layer_checker/properties.yaml properties.yaml

```