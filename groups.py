from confluent_kafka.admin import AdminClient


admin_client = AdminClient({
    'bootstrap.servers': 'localhost:9092'  # Change to your Kafka broker address
})

def get_consumer_groups():
    # Retrieve all consumer groups
    consumer_groups = admin_client.list_consumer_groups(request_timeout=10)

    # Display consumer group information
    print("Consumer groups in the Kafka cluster: ", consumer_groups)
    for group in consumer_groups:
        print(group)
    # if isinstance(consumer_groups, list):
    #     print("Consumer groups in the Kafka cluster:")
    #     for group in consumer_groups:
    #         print(f"- Group ID: {group.group_id}, State: {group.state}")
    # else:
    #     print("Unexpected return type:", type(consumer_groups))


def get_consumer_group_info():
    # Describe the specified consumer group
    group_id = 'my_consumer_group'
    future = admin_client.describe_consumer_groups([group_id])

    try:
        # Get the result for the consumer group description
        group_description = future[group_id].result()  # This will raise an exception if it fails

        print(f"Information for Consumer Group '{group_id}':")
        print(f"  Group State: {group_description.state}")
        print(f"  Protocol Type: {group_description.protocol_type}")
        print(f"  Members:")

        for member in group_description.members:
            print(f"    Member ID: {member.id}")
            print(f"    Client ID: {member.client_id}")
            print(f"    Client Host: {member.client_host}")
            print(f"    Assigned Partitions:")

            for topic, partitions in member.assignment.items():
                print(f"      Topic: {topic}, Partitions: {partitions}")

    except Exception as e:
        print(f"Failed to retrieve consumer group info for '{group_id}': {e}")


def delete_consumer_group():    
    group_id = 'my_consumer_group'
    futures = admin_client.delete_consumer_groups([group_id])

    # Handle response
    for group, future in futures.items():
        try:
            future.result()  # This will raise an exception if deletion fails
            print(f"Consumer group '{group}' deleted successfully.")
        except Exception as e:
            print(f"Failed to delete consumer group '{group}': {e}")
            

get_consumer_groups()
# get_consumer_group_info()
# delete_consumer_group()
