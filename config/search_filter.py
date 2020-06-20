import os

# TODO: Convert all field names to variables for easy updating
# args
min_date = "2020-06-01T00:00:00.000Z" #  Earliest date to search for
max_date = "2020-06-15T00:00:00.000Z" #  Latest date to search for
max_cc = 0.01
max_view_angle = 10

# Constants
dove_item_types = ['PSScene3Band']

# Location to search
search_geometry = {
    "type": "Polygon",
    "coordinates": [
                        [
                            [-149.326171875, 64.11060221954631],
                            [-146.18408203125, 64.11060221954631],
                            [-146.18408203125, 65.29346780107583],
                            [-149.326171875, 65.29346780107583],
                            [-149.326171875, 64.11060221954631]
                        ]
                    ]
                    }

geometry_filter = {
    "type": "GeometryFilter",
    "field_name": "geometry",
    "config": search_geometry
}

# Set up filters
min_date_filter = {
    "type": "DateRangeFilter",
    "field_name": "acquired",
    "config": {"gte": min_date}
    }
max_date_filter = {
    "type": "DateRangeFilter",
    "field_name": "acquired",
    "config": {"lte": max_date}
    }
date_filter = {
    "type": "AndFilter",
    "config": [min_date_filter, max_date_filter]
    }
cc_filter = {
    "type": "RangeFilter",
    "field_name": "cloud_cover",
    "config": {"lte": max_cc}
    }
view_angle_filter = {
    "type": "RangeFilter",
    "field_name": "view_angle",
    "config": {"lte": max_view_angle},
    }
quality_cat_filter = {
    "type": "StringInFilter",
    "field_name": "quality_category",
    "config": ["standard"]
}
assest_filter = {
    "type": "AssetFilter",
    "config": ["basic_analytic_dn"]
}
master_filter = {
    "type": "AndFilter",
    "config": [date_filter, cc_filter, view_angle_filter,
               quality_cat_filter, assest_filter,
               geometry_filter]
    }
# Create a search
master_search = {
    "name": "planet_stereo",
    "item_types": dove_item_types,
    "filter": master_filter,
    }