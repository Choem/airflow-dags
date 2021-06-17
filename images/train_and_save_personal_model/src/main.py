import os

from datetime import datetime

def get_graphql_client():
        transport = RequestsHTTPTransport(
            url='http://file-service:4000/file/graphql/query', 
            use_json=True,
        )

       return Client(transport=transport, fetch_schema_from_transport=True)

def main():
    user_id = os.environ['USER_ID']

    

main()