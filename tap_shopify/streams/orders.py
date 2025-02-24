import json
import os
import singer
import shopify
from singer import metadata
from singer.utils import strftime, strptime_to_utc
from tap_shopify.context import Context
from tap_shopify.streams.base import (Stream, shopify_error_handling)
import re

LOGGER = singer.get_logger()

def generate_dynamic_query(schema):
    query_parts = []
    if 'properties' in schema:
        for field, details in schema['properties'].items():
            if field.startswith('_sdc_'):
                # Handle synthetic fields differently
                continue
            elif details.get('type') and 'array' in details['type']:
                nested_parts = generate_dynamic_query(details["items"])
                query_parts.append(f"""
                    {field}(first: $first, after: $after) {{
                        edges {{
                            node {{
                                {nested_parts}
                            }}
                        }}
                        pageInfo {{
                            hasNextPage
                            endCursor
                        }}
                    }}""")
            elif details.get('type') and 'object' in details['type']:
                nested_parts = generate_dynamic_query(details)
                query_parts.append(f'{field} {{ {nested_parts} }}')
            else:
                query_parts.append(field)
    return ' '.join(query_parts)

class Orders(Stream):
    name = 'orders'
    replication_key = 'updatedAt'

    def get_schema_from_catalog(self):
        catalog_path = os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            '../../catalog.json'
        )
        with open(catalog_path) as file:
            catalog = json.load(file)
        
        for stream in catalog['streams']:
            if stream['tap_stream_id'] == self.name:
                return stream['schema']
        
        raise Exception(f"Schema for {self.name} not found in catalog.json")

    def generate_gql_query(self):
        schema = self.get_schema_from_catalog()
        fields = generate_dynamic_query(schema)
        return f"""
        query($first: Int!, $after: String, $query: String) {{
          {self.name}(first: $first, after: $after, query: $query) {{
            edges {{
              node {{
                {fields}
              }}
            }}
            pageInfo {{
              hasNextPage
              endCursor
            }}
          }}
        }}
        """

    @shopify_error_handling
    def call_api_for_stream(self, variables):
        gql_client = shopify.GraphQL()
        query = self.generate_gql_query()
        # print("GQL Query: ", query)
        response = gql_client.execute(query, variables)
        LOGGER.debug(f"GraphQL response: {response}")
        return json.loads(response)

    def camel_to_snake(self, name):
        pattern = re.compile(r'(?<!^)(?=[A-Z]|\d)')
        return pattern.sub('_', name).lower()

    def process_edges(self, data):
        if isinstance(data, dict) and 'edges' in data:
            return [self.process_edges(edge['node']) for edge in data['edges']]
        elif isinstance(data, dict):
            return {self.camel_to_snake(k): self.process_edges(v) for k, v in data.items()}
        elif isinstance(data, list):
            return [self.process_edges(item) for item in data]
        else:
            return data
        
    def get_objects(self):
        variables = {
            "first": 100,
            "after": None,
            "query": f"updated_at:>={self.get_start_date()}"
        }

        while True:
            response = self.call_api_for_stream(variables)
            
            if 'data' not in response:
                LOGGER.warning("Unexpected response format. 'data' key not found.")
                LOGGER.debug(f"Response: {response}")
                if 'errors' in response:
                    LOGGER.error(f"GraphQL errors: {response['errors']}")
                break

            stream_rows = response.get('data', {}).get(self.name, {}).get('edges', [])
            
            for stream_row in stream_rows:
                processed_row = self.process_edges(stream_row['node'])
                yield processed_row
            
            page_info = response.get('data', {}).get(self.name, {}).get('pageInfo', {})
            if not page_info.get('hasNextPage', False):
                break
            
            variables["after"] = page_info.get('endCursor')

    def get_start_date(self):
        return self.get_bookmark() or self.config.get('start_date')

    def sync(self):
        bookmark = self.get_bookmark()
        self.max_bookmark = bookmark
        
        schema = self.get_schema_from_catalog()
        singer.write_schema(self.name, schema, ['id'])

        if self.replication_key:
            self.replication_key = self.camel_to_snake(self.replication_key)

            for item in self.get_objects():
                if self.replication_key not in item:
                    LOGGER.warning(f"Replication key '{self.replication_key}' not found in order data. Skipping record.")
                    continue

                replication_value = strptime_to_utc(item[self.replication_key])
                if replication_value >= bookmark:
                    singer.write_record(self.name, item)
                    yield item  # Yield the order here
                    if replication_value > self.max_bookmark:
                        self.max_bookmark = replication_value
        else:
            for item in self.get_objects():
                singer.write_record(self.name, item)
                yield item

        singer.write_state({self.name: strftime(self.max_bookmark)})

Context.stream_objects['orders'] = Orders