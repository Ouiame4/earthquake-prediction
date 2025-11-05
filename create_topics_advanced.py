#!/usr/bin/env python3
"""
Création des topics Kafka avec retention policies conformes au projet
"""
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import time
import sys

def wait_for_kafka(max_retries=30):
    print("🔧 Attente de Kafka...")
    for i in range(max_retries):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers="localhost:9092",
                client_id='topic-creator',
                request_timeout_ms=5000
            )
            print("✅ Kafka est prêt!")
            return admin
        except Exception as e:
            if i < max_retries - 1:
                print(f"⏳ Tentative {i+1}/{max_retries}...")
                time.sleep(2)
            else:
                print(f"❌ Kafka non accessible: {e}")
                sys.exit(1)

def create_topics():
    admin = wait_for_kafka()
    
    # Topics avec retention policies selon les specs du prof
    topics = [
        # Raw data: 72 heures (259200000 ms)
        NewTopic(
            name="data.raw.earthquakes",
            num_partitions=6,
            replication_factor=1,
            topic_configs={
                'retention.ms': '259200000',  # 72h
                'compression.type': 'snappy',
                'cleanup.policy': 'delete'
            }
        ),
        # Cleaned: 7 jours (604800000 ms)
        NewTopic(
            name="data.cleaned.earthquakes",
            num_partitions=6,
            replication_factor=1,
            topic_configs={
                'retention.ms': '604800000',  # 7d
                'compression.type': 'snappy'
            }
        ),
        # Features: 7 jours
        NewTopic(
            name="data.features.hourly",
            num_partitions=6,
            replication_factor=1,
            topic_configs={
                'retention.ms': '604800000',  # 7d
                'compression.type': 'snappy'
            }
        ),
        # Predictions: 30 jours (2592000000 ms)
        NewTopic(
            name="data.predictions",
            num_partitions=3,
            replication_factor=1,
            topic_configs={
                'retention.ms': '2592000000',  # 30d
                'compression.type': 'snappy'
            }
        ),
        # Alerts: 30 jours
        NewTopic(
            name="data.alerts",
            num_partitions=3,
            replication_factor=1,
            topic_configs={
                'retention.ms': '2592000000',  # 30d
                'compression.type': 'snappy'
            }
        ),
        # Labels: 90 jours (7776000000 ms)
        NewTopic(
            name="data.labels",
            num_partitions=3,
            replication_factor=1,
            topic_configs={
                'retention.ms': '7776000000',  # 90d
                'compression.type': 'snappy'
            }
        )
    ]

    print("\n📊 Création des topics avec retention policies...")
    for topic in topics:
        try:
            admin.create_topics(new_topics=[topic], validate_only=False)
            retention_hours = int(topic.topic_configs['retention.ms']) // 3600000
            print(f"✅ {topic.name}")
            print(f"   - Partitions: {topic.num_partitions}")
            print(f"   - Retention: {retention_hours}h")
        except TopicAlreadyExistsError:
            print(f"ℹ️  {topic.name} existe déjà")
        except Exception as e:
            print(f"❌ Erreur {topic.name}: {e}")

    print("\n📋 Topics disponibles:")
    topics_list = admin.list_topics()
    for topic in sorted(topics_list):
        if topic.startswith('data.'):
            print(f"  - {topic}")
    
    admin.close()
    print("\n✅ Configuration terminée!")

if __name__ == "__main__":
    create_topics()
