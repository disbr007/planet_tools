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
```
conda env create -f environment.yml
conda activate planet_tools
```
## Setup
### Planet API Key
An environmental variable PL_API_KEY must exist with your Planet API key in 
order to query the Planet Data API and place orders.  
On Windows:
```
setx PL_API_KEY [your API key]
```
On Linux add the following to your `.bashrc` file:
```
export PL_API_KEY=[your API key]
```

### Configuration file
A number of settings, included database and AWS credentials are set 
through the use of a configuration file at: `config/config.json`. 
See `config/config_example.json` for an example.

## Usage
### Search Planet archive for footprints
Search using attribute arguments:  
```
python search4footprints.py --name my_search \ 
    --item_types PSScene4Band \
    --asset_filter basic_analytic \
    --min_date 2019-01-27 \
    --instrument PS2 \
    --aoi vectorfile.shp 
```

Search using raw filters:  
```
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
the footprints to (needs to be hardcoded to only write to `scenes`  

To get the count for a search without saving the search to your Planet 
account:
```
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

### Select footprints
Select footprints that have been saved to `scenes` table:
```
python select_footprints.py 
    --aoi vectorfile.shp \
    --min_date 2019-01-27 \
    --instrument PS2 \
    --max_cc 20 \
    --out_selection selection.shp
```

### Order and download imagery via AWS
A selected footprint (or list of IDs) can be used to order and download
imagery:
```
python order_and_download.py 
    --name my_order \
    --selection selection.shp \
    --product_bundle basic_analytic \
    --orders ordered_ids.txt 
    --destination_parent_directory orders/
``` 

### Shelving and Indexing
Once an order has been downloaded, it can be shelved and indexed:
```
python shelve_scenes.py -i orders/ --index_scenes
```

## Miscellaneous
`lib`  
A number of submodules are included to group functions that share a common purpose
* `lib.logging_utils`: The lowest level submodule functions to simplify logging. 
Highlights:
    * `logging_utils.create_logger` Create a logger of a given name, handler type, 
    and handler level. If an identical handler exists, skip duplicating. When 
    `__name__` is used as the name of the logger within each submodule, this allows
    for creating a logger anywhere that will retreive logging messages from the 
    specified submodule. For example:  
        In `lib.db.py`:  
        ```
        from lib.logging_utils import create_logger
        
        logger = create_logger(__name__, 'sh', 'INFO')
        logger.debug('A DEBUG-level message')
        ```
        In `shelve_scenes.py`
        ```
        from lib.logging_utils import create_logger
        # Create logger for main file
        logger = create_logger(__name__, 'sh', 'INFO')
        # Get logging messages from lib.db, with INFO-level writing to console
        # and DEBUG level to file.
        logger = create_logger('lib.db', 'sh', 'INFO')
        logger = create_logger('lib.db', 'fh', 'DEBUG', filename='lib.db.log')
        ```

* `lib.lib`: The second lowest level submodule. Highlights:
    * read configuration file
    * convert from terranova (`/mnt/pgc/`) paths to Windows (`V:\pgc`)
    * read ids from file
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

`fp_planet.py`  
Footprint a directory containing Planet imagery. Imagery is identified by locating
scene-level manifest files. (in progress)

`ingest_off_nadir.py`  
Parse off-nadir metadata .csv's from Planet into database table `off_nadir`. These
csv's contain Planet's internal metadata values.

`manage_searches.py`  
List or delete saved searches using the 
[Planet Data API](https://developers.planet.com/docs/apis/data/).

`multilook_selection.py`  
Select multilook 'pairs' from `multilook_candidates` table that meet minimum pairs and
minimum area arguments.
  
`scene_retriever.py`  
Copy scenes from shelved locations to a destination directory. (in progress)
  
`sort_scenes_by_date.py`  
Simple script to sort scenes by date, using only the filenames.

`submit_order.py`  
Wrapper around lib.order to provide a stand-alone method of ordering a list of IDs
or selection footprint. Can likely be removed as `order_and_download.py` can easily
be modified to just submit an order.
