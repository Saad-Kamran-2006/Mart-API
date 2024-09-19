# from confluent_kafka.schema_registry import SchemaRegistryClient
# from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
# from app.protobuf import user_pb2




# def register_protobuf_schema(schema_registry_url, subject):
#     # Create Schema Registry Client
#     schema_registry_conf = {'url': schema_registry_url}
#     schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    
#     # Define the ProtobufSerializer with user_pb2.Users class
#     protobuf_serializer = ProtobufSerializer(user_pb2.Users, schema_registry_client, conf={"use.deprecated.format": False})
    
#     # Get the serialized Protobuf schema from the User class descriptor
#     protobuf_schema = user_pb2.Users.DESCRIPTOR
    
#     try:
#         schema = schema_registry_client.get_latest_version(subject)
#         print(f"Schema already registered with ID: {schema.schema_id}")
#         return schema
#     except:
#         schema_id = schema_registry_client.register_schema(subject, protobuf_schema)
#         print(f"New schema registered with ID: {schema_id}")



























# # import requests
# # import json




# # # # Define the schema subject
# # # subject = "register-user"  # This is the name of the subject in the Schema Registry

# # # Register schema
# # def register_protobuf_schema(schema_registry_url, subject, proto_file_path):
# #     url = f"{schema_registry_url}/subjects/{subject}/versions"
# #     headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}

# #     # Read the Protobuf schema from file
# #     with open(proto_file_path, 'r') as file:
# #         protobuf_schema = file.read()

# #     data = {
# #         "schema": protobuf_schema
# #     }

# #     response = requests.post(url, headers=headers, json=data)

# #     if response.status_code == 200:
# #         print("Schema registered successfully!")
# #         print("Response:", response.json())
# #     else:
# #         print("Failed to register schema.")
# #         print("Status Code:", response.status_code)
# #         print("Response:", response.text)
