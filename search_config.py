import os

search_geometry = {"geometry": {
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
    }

# Set up filters
min_date_filter = {
    "type": "DateRangeFilter",
    "field_name": acquired,
    "config": {"gte": min_date}
    }
max_date_filter = {
    "type": "DateRangeFilter",
    "field_name": acquired,
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
    "config": [date_filter, cc_filter, view_angle_filter, quality_cat_filter, assest_filter]
    }
search_request = {
    "item_types": dove_item_types,
    "filter": master_filter,
    }