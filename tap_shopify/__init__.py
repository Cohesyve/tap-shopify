#!/usr/bin/env python3
import os
import datetime
import json
import time
import math
import copy
import logging
import requests

import pyactiveresource
import shopify
import singer
from singer import utils
from singer import metadata
from singer import Transformer
from tap_shopify.context import Context
from tap_shopify.exceptions import ShopifyError
from tap_shopify.streams.base import shopify_error_handling
import tap_shopify.streams # Load stream objects into Context

REQUIRED_CONFIG_KEYS = ["shop"]
LOGGER = singer.get_logger()
SDC_KEYS = {'id': 'integer', 'name': 'string', 'myshopify_domain': 'string'}

logging.getLogger('backoff').setLevel(logging.CRITICAL)

@shopify_error_handling
def initialize_shopify_client():
    api_key = Context.config.get('access_token') or Context.config.get('api_key')
    if api_key is None:
        raise ValueError("No 'access_token' or 'api_key' provided in the config file.")
    # Remove .myshopify.com if present in shop name
    if '.' in Context.config['shop']:
        Context.config['shop'] = Context.config['shop'].split('.')[0]
    shop = Context.config['shop']
    version = Context.config.get('api_version', '2024-01')
    session = shopify.Session(shop, version, api_key)
    shopify.ShopifyResource.activate_session(session)

    # Make GraphQL request to get shop details if not in config
    if not Context.config.get('shop_id'):
        load_shop_id(shop, api_key)
    graphql_version = Context.config.get('graphql_api_version', '2024-07')
    graphql_session = shopify.Session(Context.config["shop_id"], graphql_version, api_key)

    # Shop.current() makes a call for shop details with provided shop and api_key
    return shopify.Shop.current().attributes, session, graphql_session

def load_shop_id(shop, api_key):
    graphql_version = Context.config.get('graphql_api_version', '2024-07')
    graphql_url = f"https://{shop}.myshopify.com/admin/api/{graphql_version}/graphql.json"
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Shopify-Access-Token": api_key
    }
    query = """
    {
        shop {
            name
            id
        }
    }
    """

    response = requests.post(
        graphql_url,
        headers=headers,
        json={"query": query}
    )

    if hasattr(response, 'url'):
        # Extract shop ID from response URL
        shop_url = response.url
        Context.config['shop_id'] = shop_url.split('/')[2].split('.')[0]
    else:
        LOGGER.warning("Failed to fetch shop details via GraphQL: %s", response.text)

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)

# Load schemas from schemas folder
def load_schemas():
    schemas = {}

    # This schema represents many of the currency values as JSON schema
    # 'number's, which may result in lost precision.
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        schema_name = filename.replace('.json', '')
        with open(path) as file:
            schemas[schema_name] = json.load(file)

    return schemas


def get_discovery_metadata(stream, schema):
    mdata = metadata.new()
    mdata = metadata.write(mdata, (), 'table-key-properties', stream.key_properties)
    mdata = metadata.write(mdata, (), 'forced-replication-method', stream.replication_method)

    if stream.replication_key:
        mdata = metadata.write(mdata, (), 'valid-replication-keys', [stream.replication_key])

    for field_name in schema['properties'].keys():
        if field_name in stream.key_properties or field_name == stream.replication_key:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'automatic')
        else:
            mdata = metadata.write(mdata, ('properties', field_name), 'inclusion', 'available')

    return metadata.to_list(mdata)

def load_schema_references():
    shared_schema_file = "definitions.json"
    shared_schema_path = get_abs_path('schemas/')

    refs = {}
    with open(os.path.join(shared_schema_path, shared_schema_file)) as data_file:
        refs[shared_schema_file] = json.load(data_file)

    return refs

def add_synthetic_key_to_schema(schema):
    for k in SDC_KEYS:
        schema['properties']['_sdc_shop_' + k] = {'type': ["null", SDC_KEYS[k]]}
    return schema

def discover():
    initialize_shopify_client() # Checking token in discover mode

    raw_schemas = load_schemas()
    streams = []

    refs = load_schema_references()
    for schema_name, schema in raw_schemas.items():
        if schema_name not in Context.stream_objects:
            continue

        stream = Context.stream_objects[schema_name]()

        # resolve_schema_references() is changing value of passed refs.
        # Customer is a stream and it's a nested field of orders and abandoned_checkouts streams
        # and those 3 _sdc fields are also added inside nested field customer for above 2 stream
        # so create a copy of refs before passing it to resolve_schema_references().
        refs_copy = copy.deepcopy(refs)
        catalog_schema = add_synthetic_key_to_schema(
            singer.resolve_schema_references(schema, refs_copy))

        # create and add catalog entry
        catalog_entry = {
            'stream': schema_name,
            'tap_stream_id': schema_name,
            'schema': catalog_schema,
            'metadata': get_discovery_metadata(stream, schema),
            'key_properties': stream.key_properties,
            'replication_key': stream.replication_key,
            'replication_method': stream.replication_method
        }
        streams.append(catalog_entry)

    return {'streams': streams}

def shuffle_streams(stream_name):
    '''
    Takes the name of the first stream to sync and reshuffles the order
    of the list to put it at the top
    '''
    matching_index = 0
    for i, catalog_entry in enumerate(Context.catalog["streams"]):
        if catalog_entry["tap_stream_id"] == stream_name:
            matching_index = i
    top_half = Context.catalog["streams"][matching_index:]
    bottom_half = Context.catalog["streams"][:matching_index]
    Context.catalog["streams"] = top_half + bottom_half

# pylint: disable=too-many-locals
def sync():
    shop_attributes, rest_session, graphql_session = initialize_shopify_client()
    sdc_fields = {"_sdc_shop_" + x: shop_attributes[x] for x in SDC_KEYS}
    Context.shopify_graphql_session = graphql_session
    Context.shopify_rest_session = rest_session
    shopify.ShopifyResource.activate_session(rest_session)


    # Emit all schemas first so we have them for child streams
    for stream in Context.catalog["streams"]:
        if Context.is_selected(stream["tap_stream_id"]):
            if stream.get("replication_key"):
                singer.write_schema(stream["tap_stream_id"],
                                    stream["schema"],
                                    stream["key_properties"],
                                    bookmark_properties=stream["replication_key"])
            else:
                singer.write_schema(stream["tap_stream_id"],
                                    stream["schema"],
                                    stream["key_properties"])

            Context.counts[stream["tap_stream_id"]] = 0

    # If there is a currently syncing stream bookmark, shuffle the
    # stream order so it gets sync'd first
    currently_sync_stream_name = Context.state.get('bookmarks', {}).get('currently_sync_stream')
    if currently_sync_stream_name:
        shuffle_streams(currently_sync_stream_name)

    # Loop over streams in catalog
    for catalog_entry in Context.catalog['streams']:
        stream_id = catalog_entry['tap_stream_id']
        stream = Context.stream_objects[stream_id]()

        if not Context.is_selected(stream_id):
            LOGGER.info('Skipping stream: %s', stream_id)
            continue

        LOGGER.info('Syncing stream: %s', stream_id)

        if not Context.state.get('bookmarks'):
            Context.state['bookmarks'] = {}
        Context.state['bookmarks']['currently_sync_stream'] = stream_id

        with Transformer() as transformer:
            for rec in stream.sync():
                extraction_time = singer.utils.now()
                record_schema = catalog_entry['schema']
                record_metadata = metadata.to_map(catalog_entry['metadata'])
                rec = transformer.transform({**rec, **sdc_fields},
                                            record_schema,
                                            record_metadata)
                singer.write_record(stream_id,
                                    rec,
                                    time_extracted=extraction_time)
                Context.counts[stream_id] += 1

        Context.state['bookmarks'].pop('currently_sync_stream')
        singer.write_state(Context.state)

    LOGGER.info('----------------------')
    for stream_id, stream_count in Context.counts.items():
        LOGGER.info('%s: %d', stream_id, stream_count)
    LOGGER.info('----------------------')

@utils.handle_top_exception(LOGGER)
def main():
    try:
        # Parse command line arguments
        args = utils.parse_args(REQUIRED_CONFIG_KEYS)

        Context.config = args.config
        Context.state = args.state

        # If discover flag was passed, run discovery mode and dump output to stdout
        if args.discover:
            catalog = discover()
            print(json.dumps(catalog, indent=2))
        # Otherwise run in sync mode
        else:
            Context.tap_start = utils.now()
            if args.catalog:
                Context.catalog = args.catalog.to_dict()
            else:
                Context.catalog = discover()

            sync()
    except pyactiveresource.connection.ResourceNotFound as exc:
        raise ShopifyError(exc, 'Ensure shop is entered correctly') from exc
    except pyactiveresource.connection.UnauthorizedAccess as exc:
        raise ShopifyError(exc, 'Invalid access token - Re-authorize the connection') \
            from exc
    except pyactiveresource.connection.ConnectionError as exc:
        msg = ''
        try:
            body_json = exc.response.body.decode()
            body = json.loads(body_json)
            msg = body.get('errors')
        finally:
            raise ShopifyError(exc, msg) from exc
    except Exception as exc:
        raise ShopifyError(exc) from exc

if __name__ == "__main__":
    main()
