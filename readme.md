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
An environmental variable PL_API_KEY must exist with your Planet API key.  
On Windows:
```
setx PL_API_KEY [your API key]
```
On Linux add the following to your `.bashrc` file:
```
export PL_API_KEY=[your API key]
```

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
<br />

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
<br />

To get the count for a search without saving the search to your Planet 
account:
```
python search4footprints.py --name my_search \ 
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
python select_footprints.py --aoi vectorfile.shp \
    --min_date 2019-01-27 \
    --instrument PS2 \
    --max_cc 20 \
    --out_selection selection.shp
```

### Order and download imagery via AWS
A selected footprint (or list of IDs) can be used to order and download
imagery:
```
python order_and_download.py --name my_order \
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


## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what 
you would like to change.

