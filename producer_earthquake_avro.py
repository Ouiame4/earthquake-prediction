#!/usr/bin/env python3
"""
Producer avec Schema Registry et Avro
Utilise confluent-kafka pour l'intégration Avro native
"""
import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from datetime import datetime
import time
import sys

# ============= CONFIGURATION =============
USGS_API = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_hour.geojson"
KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_RAW = "data.raw.earthquakes"
POLL_INTERVAL = 300  # 5 minutes

# ============= PRODUCER MODERNE =============
def create_avro_producer():
    """Crée un producer avec AvroSerializer (méthode moderne)"""
    max_retries = 10
    
    for attempt in range(max_retries):
        try:
            # Configurer Schema Registry Client
            schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
            schema_registry_client = SchemaRegistryClient(schema_registry_conf)
            
            # Charger le schéma depuis le Schema Registry
            schema_response = requests.get(
                f"{SCHEMA_REGISTRY_URL}/subjects/{TOPIC_RAW}-value/versions/latest"
            )
            
            if schema_response.status_code != 200:
                raise Exception(f"Schéma non trouvé: {schema_response.text}")
            
            schema_data = schema_response.json()
            value_schema_str = schema_data['schema']
            
            print(f"✅ Schéma chargé (ID: {schema_data['id']}, Version: {schema_data['version']})")
            
            # Créer les serializers
            string_serializer = StringSerializer('utf_8')
            avro_serializer = AvroSerializer(
                schema_registry_client,
                value_schema_str
            )
            
            # Créer le producer
            producer_conf = {
                'bootstrap.servers': KAFKA_BOOTSTRAP,
                'compression.type': 'snappy',
                'acks': 'all',
                'retries': 3,
                'linger.ms': 10,
                'batch.size': 16384
            }
            
            producer = Producer(producer_conf)
            
            print("✅ Producer Avro connecté")
            return producer, string_serializer, avro_serializer
            
        except Exception as e:
            if attempt < max_retries - 1:
                print(f"⏳ Tentative {attempt + 1}/{max_retries}... ({e})")
                time.sleep(3)
            else:
                print(f"❌ Impossible de créer le producer: {e}")
                sys.exit(1)

# ============= FETCH DATA =============
def fetch_earthquake_data():
    """Récupère les données de l'API USGS"""
    try:
        print(f"📡 API Call: {datetime.now().strftime('%H:%M:%S')}")
        response = requests.get(USGS_API, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f"✅ Reçu: {data['metadata']['count']} événements")
        return data
    except Exception as e:
        print(f"❌ Erreur API: {e}")
        return None

# ============= DELIVERY CALLBACK =============
def delivery_report(err, msg):
    """Callback appelé après chaque message"""
    if err is not None:
        print(f"❌ Erreur delivery: {err}")
    else:
        print(f"✅ P{msg.partition()} O{msg.offset()} | {msg.key().decode('utf-8') if msg.key() else 'no-key'}")

# ============= PROCESS & SEND =============
def process_and_send(earthquake, producer, string_serializer, avro_serializer, seen_ids):
    """Traite et envoie un événement avec Avro"""
    try:
        event_id = earthquake['id']
        
        if event_id in seen_ids:
            return False
        seen_ids.add(event_id)
        
        props = earthquake['properties']
        geom = earthquake['geometry']
        
        # Validation
        if props.get('mag') is None or geom.get('coordinates') is None:
            return False
        
        # Key en string
        location = props.get('place', 'unknown').replace('|', '-')
        key = f"usgs|{location}|{event_id}"
        
        # Mapper vers le schéma Avro
        avro_message = {
            'event_id': event_id,
            'timestamp': props['time'],
            'timestamp_updated': props.get('updated'),
            'magnitude': props.get('mag'),
            'magnitude_type': props.get('magType'),
            'location': props.get('place'),
            'depth_km': geom['coordinates'][2] if len(geom['coordinates']) > 2 else None,
            'latitude': geom['coordinates'][1] if len(geom['coordinates']) > 1 else None,
            'longitude': geom['coordinates'][0] if len(geom['coordinates']) > 0 else None,
            'alert_level': props.get('alert', 'none') or 'none',
            'felt_reports': props.get('felt'),
            'cdi': props.get('cdi'),
            'mmi': props.get('mmi'),
            'tsunami': props.get('tsunami', 0),
            'significance': props.get('sig'),
            'network': props.get('net'),
            'source': 'usgs',
            'ingestion_timestamp': datetime.now().isoformat(),
            'api_version': '1.0'
        }
        
        # Sérialiser la clé et la valeur
        serialized_key = string_serializer(key)
        serialized_value = avro_serializer(
            avro_message, 
            SerializationContext(TOPIC_RAW, MessageField.VALUE)
        )
        
        # Envoyer
        producer.produce(
            topic=TOPIC_RAW,
            key=serialized_key,
            value=serialized_value,
            callback=delivery_report
        )
        
        # Poll pour traiter les callbacks
        producer.poll(0)
        
        return True
        
    except Exception as e:
        print(f"❌ Erreur process: {e}")
        import traceback
        traceback.print_exc()
        return False

# ============= MAIN =============
def main():
    print("=" * 70)
    print("🌍 EARTHQUAKE PRODUCER WITH AVRO + SCHEMA REGISTRY")
    print("=" * 70)
    print(f"📡 Source: {USGS_API}")
    print(f"📊 Topic: {TOPIC_RAW}")
    print(f"📝 Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"⏱️ Intervalle: {POLL_INTERVAL}s ({POLL_INTERVAL//60} minutes)")
    print("=" * 70)
    
    producer, string_serializer, avro_serializer = create_avro_producer()
    seen_ids = set()
    
    total_sent = 0
    iteration = 0
    
    try:
        while True:
            iteration += 1
            print(f"\n{'='*70}")
            print(f"🔄 ITERATION #{iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print(f"{'='*70}")
            
            data = fetch_earthquake_data()
            
            if data and 'features' in data:
                earthquakes = data['features']
                sent_count = 0
                
                for eq in earthquakes:
                    if process_and_send(eq, producer, string_serializer, avro_serializer, seen_ids):
                        sent_count += 1
                        total_sent += 1
                
                # Flush pour s'assurer que tous les messages sont envoyés
                print("\n📤 Flush des messages...")
                producer.flush(timeout=10)
                
                print(f"\n📈 Statistiques:")
                print(f"   ├─ Nouveaux événements: {sent_count}")
                print(f"   ├─ Doublons ignorés: {len(earthquakes) - sent_count}")
                print(f"   ├─ Total envoyé: {total_sent}")
                print(f"   └─ Cache IDs: {len(seen_ids)}")
            else:
                print("⚠️ Aucune donnée reçue de l'API")
            
            print(f"\n⏳ Pause de {POLL_INTERVAL//60} minutes...")
            time.sleep(POLL_INTERVAL)
            
    except KeyboardInterrupt:
        print("\n\n🛑 Arrêt demandé par l'utilisateur")
    except Exception as e:
        print(f"\n❌ Erreur fatale: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print("\n🔒 Fermeture du producer...")
        producer.flush(timeout=10)
        print(f"✅ Session terminée - {total_sent} événements envoyés au total")
        print("=" * 70)

if __name__ == "__main__":
    main()