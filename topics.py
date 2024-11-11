import json
from confluent_kafka.admin import AdminClient, ConfigResource, NewTopic, NewPartitions

# Create an AdminClient instance and configure the broker
admin_client = AdminClient({
    'bootstrap.servers': 'localhost:9092'  # Change to your Kafka broker address
})


def get_topic_list():
    try:
        topics = admin_client.list_topics(timeout=10)  # timeout in seconds
        for topic, meta_data in topics.topics.items():
            print(f'Topic: {topic} | Partitions: {len(meta_data.partitions)}')
    except Exception as e:
        print(f"Error listing topics: {e}")
        

def get_topic_info():
    topic_name = 'connect-statuses'
    config_resource = ConfigResource(ConfigResource.Type.TOPIC, topic_name)
    future = admin_client.describe_configs([config_resource])

    try:
        # Fetch configuration result
        topic_configs = future[config_resource].result()
        
        # Parse topic configuration details
        topic_info = {
            'topic': topic_name,
            'configurations': {k: v.value for k, v in topic_configs.items()}
        }

        # Output topic details
        print(json.dumps(topic_info, indent=4))
        
    except Exception as e:
        print(f"Failed to retrieve topic info: {e}")


def create_topic():
    topic_name = 'my_topic'
    num_partitions = 3
    replication_factor = 1
    configs = {
        'cleanup.policy': 'compact',       # e.g., 'delete' or 'compact'
        'retention.ms': '604800000',       # 7 days in milliseconds
        'segment.bytes': '104857600'       # 100 MB
    }

    new_topic = NewTopic(
        topic=topic_name,
        num_partitions=num_partitions,
        replication_factor=replication_factor,
        config=configs
    )

    # Create topic (it returns a dict with futures for each topic creation)
    futures = admin_client.create_topics([new_topic])

    # Handle response
    for topic, future in futures.items():
        try:
            future.result()  # If topic creation fails, this will raise an exception
            print(f"Topic '{topic}' created successfully.")
        except Exception as e:
            print(f"Failed to create topic '{topic}': {e}")
            

def add_partitions():
    topic_name = 'my_new_topic'
    new_total_partitions = 6  # Set the new total number of partitions

    new_partitions = NewPartitions(
        topic=topic_name,
        new_total_count=new_total_partitions
    )

    # Create new partitions (it returns a dict with futures for each topic)
    futures = admin_client.create_partitions([new_partitions])

    # Handle response
    for topic, future in futures.items():
        try:
            future.result()  # If partition creation fails, this will raise an exception
            print(f"Partitions for topic '{topic}' increased to {new_total_partitions}.")
        except Exception as e:
            print(f"Failed to add partitions to topic '{topic}': {e}")


def delete_topic():
    topic_name = 'my_new_topic'
    # Delete the specified topic (returns a dict of futures for each deletion)
    futures = admin_client.delete_topics([topic_name], operation_timeout=30)

    # Handle response
    for topic, future in futures.items():
        try:
            future.result()  # If deletion fails, this will raise an exception
            print(f"Topic '{topic}' deleted successfully.")
        except Exception as e:
            print(f"Failed to delete topic '{topic}': {e}")
            

# get_topic_list()
# get_topic_info()
# create_topic()
# add_partitions()
# delete_topic()
