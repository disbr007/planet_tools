# planet_stereo

This repo utilizes the Planet API to facilitate selecting and ordering imagery, specifically focusing on
identfying scenes that are candidates for stereo DEM production. The primary uses are:
1. Creating a "search request" based on a set of filters
2. Getting a footprint or scene count from a search request
3. Ordering and downloading imagery.

## Installation

Using conda:
```
conda env create -f planet_stereo.yml
conda activate planet_stereo
```

## Usage
To create a search from the command line:  
```
python create_saved_search.py --name my_search --item_types PSScene4Band \
    -f DateRangeFilter acquired gte 2019-01-27 \
    -f StringInFilter instrument PS2 \
    -f GeometryFilter C:\path\to\vectorfile.shp
```
To get the count for a set of filters without creating a search:
```
python create_saved_search.py --name my_search --item_types PSScene4Band \
    -f DateRangeFilter acquired gte 2019-01-27 \ 
    -f GeometryFilter C:\path\to\vectorfile.shp \
    --get_count \
    --dryrun
```
Once a search is successfully created, it will return a search ID. This can be used with 
select_imagery.py to create a footprint of the imagery:
```
python select_imagery.py -i [search ID] --out_path C:\path\to\write\footprint.shp
```
Alternatively, the fooprint can be piped directly into a Postgres databse:
```
python select_imagery.py -i [search ID] --to_tbl [table_name]
```
This requires creating the following file at *config/db_config.json*:
```python
{
  "host": [host],
  "database": [database name], 
  "user": [username],
  "password": [password]
}
```
*Completion of ordering tool underway...*

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what 
you would like to change.
