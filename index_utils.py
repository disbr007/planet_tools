import xml.etree.ElementTree as ET


def tag_uri_and_name(elem):
    if elem.tag[0] == '{':
        uri, _, name = elem.tag[1:].partition('}')
    else:
        uri = None
        name = elem.tag

    return uri, name


def add_renamed_attributes(node, renamer, root, attributes):
    elems = root.find('.//{}'.format(node))
    for e in elems:
        uri, name = tag_uri_and_name(e)
        if name in renamer.keys():
            name = renamer[name]
        if e.text.strip() != '':
            # print('{}: {}'.format(name, e.text))
            attributes[name] = e.text


def parse_xml(xml):
    with open(xml, 'rt') as src:
        tree = ET.parse(xml)
        root = tree.getroot()

    # Nodes where all values can be processed as-is
    nodes_process_all = ['{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}EarthObservationMetaData',
                         '{http://www.opengis.net/gml}TimePeriod',
                         '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}Sensor',
                         '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}Acquisition',
                         '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}ProductInformation',
                         '{http://earth.esa.int/opt}cloudCoverPercentage',
                         '{http://earth.esa.int/opt}cloudCoverPercentageQuotationMode',
                         '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}unusableDataPercentage']

    # Nodes with conflicting attribute names -> rename according to dicts
    platform_node =   '{http://earth.esa.int/eop}Platform'
    instrument_node = '{http://earth.esa.int/eop}Instrument'
    mask_node =       '{http://earth.esa.int/eop}MaskInformation'

    platform_renamer =  {'shortName': 'platform'}
    insrument_renamer = {'shortName': 'instrument'}
    mask_renamer =      {'fileName': 'mask_filename',
                         'type': 'mask_type',
                         'format': 'mask_format',
                         'referenceSystemIdentifier': 'mask_referenceSystemIdentifier'}

    rename_nodes = [(platform_node,   platform_renamer),
                    (instrument_node, insrument_renamer),
                    (mask_node,       mask_renamer)]

    # Bands Node - conflicting attribute names -> add band number: "band1_radiometicScaleFactor"
    bands_node = '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}bandSpecificMetadata'
    band_number_node = '{http://schemas.planet.com/ps/v1/planet_product_metadata_geocorrected_level}bandNumber'

    attributes = dict()
    # Add attributes that are processed as-is
    for node in nodes_process_all:
        elems = root.find('.//{}'.format(node))
        for e in elems:
            uri, name = tag_uri_and_name(e)
            if e.text.strip() != '':
                # print('{}: {}'.format(name, e.text))
                attributes[name] = e.text

    # Add attributes that require renaming
    for node, renamer in rename_nodes:
        add_renamed_attributes(node, renamer, root=root, attributes=attributes)

    # Process band metadata
    bands_elems = root.findall('.//{}'.format(bands_node))

    for band in bands_elems:
        band_uri = '{{{}}}'.format(tag_uri_and_name(band)[0])
        band_number = band.find('.//{}'.format(band_number_node)).text
        band_renamer = {tag_uri_and_name(e)[1]: 'band{}_{}'.format(band_number, tag_uri_and_name(e)[1])
                        for e in band
                        if tag_uri_and_name(e)[1] != 'bandNumber'}
        for e in band:
            band_uri, name = tag_uri_and_name(e)
            if name in band_renamer.keys():
                name = band_renamer[name]

            name = name.replace('"').replace("'")
            if e.text.strip() != '' and name != 'bandNumber':
                attributes[name] = e.text

    # TODO: Remove quotes from keys
    return attributes
