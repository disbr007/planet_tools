# planet_tools

This repository utilizes the Planet API to facilitate selecting and ordering 
imagery, including identfying scenes that are candidates for multilook 
stereo DEM production.  

**Primary workflow tools**
1. Locate footprints matching specified attributes 
    in the Planet archives (search4footprints.py)  
2. Ordering and downloading imagery. (order_and_download.py)
3. Shelving and indexing imagery. (shelve_scenes.py)
4. Retrieve on hand imagery using either a list of IDs or 
    a footprint of the index table (scene_retreiver.py)

**Additional tools:**
* Footprint a directory of Planet scenes (fp_planet.py)
* Ingest off-nadir metadata files that are provided by Planet in 
    csv format (ingest_off_nadir.py)
* Manage existing saved searches for your Planet account, including 
    listing searches, writing to json file, deleting searches. 
    (manage_searches.py)  
* Locate multilook 'pairs' from multilook candidates table 
    (multilook_selection.py)
* Select footprints from either the footprints or index tables based
    on attibutes (select_footprints.py)


## Installation

Using conda:
```commandline
conda env create -f environment.yml
conda activate planet_tools
```
## Setup
### Planet API Key
An environmental variable PL_API_KEY must exist with your Planet API key in 
order to query the Planet Data API and place orders.  
On Windows:
```commandline
setx PL_API_KEY [your API key]
```
On Linux add the following to your `.bashrc` file:
```commandline
export PL_API_KEY=[your API key]
```

### Configuration file
A number of settings, including database and AWS credentials are set 
through the use of a configuration file at: `config/config.json`. 
See `config/config_example.json` for an example.

## Usage
### Search Planet archives for footprints (search4footprints.py)
This tool searches the [Planet Data API](https://developers.planet.com/docs/apis/data/)
for footprints matching the arguments provided. The API returns the footprints 
and metadata in pages of 250 records, which can take a while to parse. 
*Note:* There is a limit to the payload size that can get POSTed when performing a
search, making it impossible to remove all onhand or previously searched for
records during the search itself. Therefore, it would be a best practice to 
track what searches (and subsequent matching footprints) have been performed 
and written to the `scenes` table and keep search parameters narrow to 
avoid repeated parsing of records. Duplicates will however be removed before 
writing to the 'scenes' table.  

The footprint written out as `--out_path` can be passed to 
`order_and_download.py` for ordering and downloading.

Search using attribute arguments:  
```commandline
python search4footprints.py --name my_search \ 
    --item_types PSScene4Band \
    --asset_filter basic_analytic \
    --min_date 2019-01-27 \
    --instrument PS2 \
    --aoi vectorfile.shp 
```

Search using raw filters:  
```commandline
python search4footprints.py --name my_search \ 
    --item_types PSScene4Band \
    --asset_filter basic_analytic \
    -f DateRangeFilter acquired gte 2019-01-27 \
    -f StringInFilter instrument PS2 \
    -f GeometryFilter vectorfile.shp
```
where the general syntax is:
```
[type of filter] [field to filter] [comparison (gte, lte, gt, lt)] [value]
```
The above two searches are identical. The footprints returned by the search
criteria can be saved using any of the following arguments:  
* `-op`: the path to write the vector file to
* `-od`: the directory to write the vector file to, where the file name 
will be the name of the search
* `--to_tbl`: the name of the table in `sandwich-pool.planet` to write
the footprints to

To get the count for a search without saving the search to your Planet 
account:
```commandline
python search4footprints.py 
    --name my_search \ 
    --item_types PSScene4Band \
    --asset_filter basic_analytic \
    --min_date 2019-01-27 \
    --instrument PS2 \
    --aoi vectorfile.shp \
    --get_count \
    --dryrun
```

### Select footprints (select_footprints.py)
This tool searches the `scenes` table for scenes matching the arguments provided
and is thus used when the user knows the records they are searching for have already
been parsed from the Planet Data API and written to the `scenes` table via 
`search4footprints.py`. Records can then be retrieved much more quickly than 
would be from parsing responses from the Data API. 
This could potentially be combined into `search4footprints.py` as an argument 
flag, denoting that `scenes` should be parsed rather than the Data API.

The footprint written out as `--out_selection` can be passed to 
`order_and_download.py` for ordering and downloading.
```commandline
python select_footprints.py 
    --aoi vectorfile.shp \
    --min_date 2019-01-27 \
    --instrument PS2 \
    --max_cc 20 \
    --out_selection selection.shp
```

### Order and download imagery (order_and_download.py)
A selected footprint (or list of IDs) can be used to order and download
imagery. Orders can only contain 500 IDs each, so this will split the input
into groups of 500 and submit them as separate orders.
*Note:* When writing this script (and using AWS) orders were completing in roughly 10 minutes,
so it made sense to link ordering and downloading. Recently (using deliver via ZIP, though 
that may not be the reason) orders have been taking multiple hours to complete. As such, it
may make sense to entirely decouple ordering and downloading. Currently, orders can just be placed 
and downloads not attempted using the `--order_only` flag. This should be combined with 
writing the order IDs to a text file using `--orders`. That text file can then be passed 
to `--download_orders` on a subsequent run to skip directly to downloading. 
Additional functionality can be developed to:
 1. notify when orders are ready via email (this exists in the Orders API).
 2. download all undownloaded orders (order status can be checked via API and compared to a 
 destination directory to determine which are new) 
 
```commandline
python order_and_download.py 
    --name my_order \
    --selection selection.shp \
    --product_bundle basic_analytic \
    --orders ordered_ids.txt 
    --destination_parent_directory orders/
    --delivery zip
``` 
Delivery via AWS is also possible, using `--delivery aws`, but requires 
that AWS account credentials are present in `config/config.json`

### Shelving and Indexing (shelve_and_index.py)
Once an order has been downloaded, it can be shelved and indexed. 
This is a routine linking shelving and indexing. In a standard run, a directory is
parsed for order manifests. The order manifest's are parsed for sections
corresponding to the actual imagery (as opposed to the metadata files, masks,
etc.), and these sections are written to new 'scene manifest' files following
the naming convention of the scenes. Next, all scenes are located based on the
presence of these scene manifest files, and their metadata (XML and scene
manifest) is parsed in order to create both the shelved path and the row to
add to the index table. Next the copy to the shelving location is performed,
with options to remove the source files after copying and/or to move any
unshelveable scenes to an alternate location. Finally, the new records are
written to the index table: planet.scenes
```commandline
python shelve_and_index.py -i orders/
```

### Multilook Stereo Selection (multilook_selection.py)
Select multilook 'pairs' (really groups of scenes) from `multilook_candidates` table that meet 
minimum number of scene and minimum area (in m<sup>2</sup>) arguments. Note this can take a long
time and it is recommended to use an AOI to reduce the selection.
```python
python multilook_selection.py --out_ids multilook_ids.txt --out_overlaps multilook_overlaps.shp 
--aoi my_aoi.shp --min_pairs 3 --min_area 32_000_000
```

### Footprint a directory of scenes
`fp_planet.py`  
Footprint a directory containing Planet imagery. A field is added to the 
footprint, specifying the path to each scene, relative to the directory provided 
to `--relative_directory`. If not directory is provided, the paths will be 
relative to the `--input_directory`.  
*Note:* Imagery is identified by locating scene-level manifest files, like 
`[scene]_manifest.json`.


## Miscellaneous
`lib`  
A number of submodules are included to group functions that share a common purpose.
* `lib.logging_utils`: Functions to simplify logging. 
Highlights:
    * `logging_utils.create_logger` Create a logger of a given name, handler type, 
    and handler level. If an identical handler exists, skip duplicating. When 
    `__name__` is used as the name of the logger within each submodule, this allows
    for creating a logger anywhere that will retreive logging messages from the 
    specified submodule. For example:  
        In `lib.db.py`:  
        ```python
        from lib.logging_utils import create_logger
        
        logger = create_logger(__name__, 'sh', 'INFO')
        logger.debug('A DEBUG-level message')
        ```
        In `shelve_scenes.py`
        ```python
        from lib.logging_utils import create_logger
        # Create logger for main file
        logger = create_logger(__name__, 'sh', 'INFO')
        # Get logging messages from lib.db, with INFO-level writing to console
        # and DEBUG level to file.
        logger = create_logger('lib.db', 'sh', 'INFO')
        logger = create_logger('lib.db', 'fh', 'DEBUG', filename='lib.db.log')
        ```

* `lib.constants`: Constant string variables, including database and table 
names, field names, keys in configuration files, etc. Generally, if the string
variable/constant is used in more than one script, it *should* be in this file. 
Other string variables, like argparse defaults, that are only used in one place
are at the top of their respective scripts. 

* `lib.lib`: General purpose functions. Highlights:
    * read configuration file
    * convert from Terranova (`/mnt/pgc/`) paths to Windows (`V:\pgc`) (PGC specific)
    * read scene ids from a variety of file source types (.txt, .csv, .shp, .geojson, etc.)
    * verify checksums
    * **PlanetScene**: a class to manage parsing attributes from metadata files,
        determining if scenes are shelveable, and indexing

* `lib.db`: Functions related to interacting with Postgres databases, namely
sandwich-pool.planet, highlights:
    * **Postgres**: a class for managing database interactions
    * managing SQL queries, including handling geometry columns during SELECT and 
        INSERT operations

* `lib.search`: Functions for searching for and selecting imagery footprints using
the [Planet Data API](https://developers.planet.com/docs/apis/data/).

* `lib.order`: Functions for ordering imagery using the 
[Planet Orders API](https://developers.planet.com/docs/orders/).

* `lib.aws_utils`: Functions for working with AWS:
    * connecting to an AWS bucket
    * checking for existence of *_manifest.json file (indicates delivery to bucket is complete)
    * creating an aws_delivery dictionary containing all necessary parameters to pass to the 
      [Planet Orders API](https://developers.planet.com/docs/orders/)


`ingest_off_nadir.py`  
Parse off-nadir metadata .csv's from Planet into database table `off_nadir`. These
csv's contain Planet's internal metadata values for each scene's off-nadir and azimuth

`manage_searches.py`  
List or delete saved searches using the 
[Planet Data API](https://developers.planet.com/docs/apis/data/).

  
`scene_retriever.py`  
Copy scenes from shelved locations to a destination directory. (in progress)
  
`sort_scenes_by_date.py`  
Simple script to sort scenes by date, using only the filenames.

`submit_order.py`  
Wrapper around lib.order to provide a stand-alone method of ordering a list of IDs
or selection footprint. Can likely be removed as `order_and_download.py` can easily
be modified to just submit an order.

### SQL
`sql\table_views_generation.sql`: Contains the SQL statements to create all tables and
views on `sandwich-pool.planet`. **Not meant to be run as a standalone script.**


### TODO:
- [] Use Planet API key from config.json, not a system environmental variable
- [x] Make boto3 import dependent on delivery method
- [] Locate scenes in `fp_planet.py` using a string or strings provided as an 
     argument for example: "*AnalyticMS.tif" Some functionality to his end 
     has been added to lib.PlanetScene already.