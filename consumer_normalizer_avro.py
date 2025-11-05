#!/usr/bin/env python3
"""
Consumer Normalizer avec Avro - Version ML-Ready
Corrige les profondeurs négatives et ajoute des features pour le ML
"""
from confluent_kafka import Consumer, Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer, StringDeserializer
from datetime import datetime
import requests
import sys
import time
from collections import defaultdict

# ============= CONFIGURATION =============
KAFKA_BOOTSTRAP = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
TOPIC_RAW = "data.raw.earthquakes"
TOPIC_CLEANED = "data.cleaned.earthquakes"
CONSUMER_GROUP = "normalizer-avro-group"

# Cache pour calculer le temps depuis le dernier événement par région
last_event_by_region = {}
event_count_by_region = defaultdict(int)

# ============= HELPER FUNCTIONS =============
def safe_float(value, default=0.0):
    """Convertir en float avec gestion des None"""
    if value is None:
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def safe_int(value, default=0):
    """Convertir en int avec gestion des None"""
    if value is None:
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

def extract_region(location_str):
    """Extrait la région depuis la location string"""
    if not location_str:
        return "unknown"
    
    # Extraire le nom du pays/région après la virgule
    parts = location_str.split(',')
    if len(parts) >= 2:
        region = parts[-1].strip()
    else:
        # Si pas de virgule, prendre les derniers mots
        words = location_str.split()
        if len(words) >= 2:
            region = ' '.join(words[-2:])
        else:
            region = location_str
    
    # Nettoyer
    region = region.replace('of ', '').strip()
    return region[:50] if region else "unknown"  # Limite à 50 caractères

def fix_depth(depth_km):
    """
    Corrige les profondeurs invalides
    - Si depth < 0 : considérer comme erreur de mesure, utiliser valeur absolue
    - Si depth > 700 : plafonner à 700 (limite du manteau)
    """
    if depth_km < 0:
        print(f"   ⚠️  Profondeur négative détectée: {depth_km} km → Corrigée à {abs(depth_km)} km")
        return abs(depth_km)
    
    if depth_km > 700:
        print(f"   ⚠️  Profondeur anormale: {depth_km} km → Plafonnée à 700 km")
        return 700.0
    
    return depth_km

def calculate_time_since_last_event(region, current_timestamp):
    """Calcule le temps écoulé depuis le dernier événement dans cette région"""
    global last_event_by_region
    
    if region in last_event_by_region:
        last_time = last_event_by_region[region]
        time_diff_seconds = (current_timestamp - last_time) / 1000.0  # ms to seconds
        time_diff_minutes = time_diff_seconds / 60.0
        last_event_by_region[region] = current_timestamp
        return round(time_diff_minutes, 2)
    else:
        last_event_by_region[region] = current_timestamp
        return None  # Premier événement dans cette région

# ============= CONSUMER MODERNE =============
def create_avro_consumer():
    """Crée un consumer avec AvroDeserializer (API moderne)"""
    try:
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        schema_response = requests.get(
            f"{SCHEMA_REGISTRY_URL}/subjects/{TOPIC_RAW}-value/versions/latest"
        )
        
        if schema_response.status_code != 200:
            raise Exception(f"Schéma raw non trouvé: {schema_response.text}")
        
        schema_data = schema_response.json()
        value_schema_str = schema_data['schema']
        
        print(f"✅ Schéma raw chargé (ID: {schema_data['id']})")
        
        string_deserializer = StringDeserializer('utf_8')
        avro_deserializer = AvroDeserializer(
            schema_registry_client,
            value_schema_str
        )
        
        consumer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP,
            'group.id': CONSUMER_GROUP,
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe([TOPIC_RAW])
        
        print(f"✅ Consumer connecté au topic: {TOPIC_RAW}")
        return consumer, string_deserializer, avro_deserializer
        
    except Exception as e:
        print(f"❌ Erreur consumer: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

# ============= PRODUCER MODERNE =============
def create_avro_producer():
    """Crée un producer avec AvroSerializer (API moderne)"""
    try:
        schema_registry_conf = {'url': SCHEMA_REGISTRY_URL}
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        schema_response = requests.get(
            f"{SCHEMA_REGISTRY_URL}/subjects/{TOPIC_CLEANED}-value/versions/latest"
        )
        
        if schema_response.status_code != 200:
            raise Exception(f"Schéma cleaned non trouvé: {schema_response.text}")
        
        schema_data = schema_response.json()
        value_schema_str = schema_data['schema']
        
        print(f"✅ Schéma cleaned chargé (ID: {schema_data['id']})")
        
        string_serializer = StringSerializer('utf_8')
        avro_serializer = AvroSerializer(
            schema_registry_client,
            value_schema_str
        )
        
        producer_conf = {
            'bootstrap.servers': KAFKA_BOOTSTRAP,
            'compression.type': 'snappy',
            'acks': 'all',
            'retries': 3
        }
        
        producer = Producer(producer_conf)
        
        print(f"✅ Producer connecté au topic: {TOPIC_CLEANED}")
        return producer, string_serializer, avro_serializer
        
    except Exception as e:
        print(f"❌ Erreur producer: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

# ============= NORMALIZATION ML-READY =============
def normalize_earthquake(raw_data):
    """Normalise les données pour le Machine Learning"""
    try:
        # Validation de base
        if not raw_data.get('event_id'):
            return None
        
        magnitude = raw_data.get('magnitude')
        if magnitude is None:
            return None
        magnitude = safe_float(magnitude)
        
        # Extraction des données brutes
        timestamp_ms = safe_int(raw_data.get('timestamp'), int(time.time() * 1000))
        dt = datetime.fromtimestamp(timestamp_ms / 1000)
        
        latitude = safe_float(raw_data.get('latitude'), 0.0)
        longitude = safe_float(raw_data.get('longitude'), 0.0)
        
        # 🔧 CORRECTION DE LA PROFONDEUR
        raw_depth = safe_float(raw_data.get('depth_km'), 0.0)
        depth_km = fix_depth(raw_depth)
        
        location_str = raw_data.get('location') or 'unknown'
        region = extract_region(location_str)
        
        # Incrémenter le compteur d'événements pour cette région
        event_count_by_region[region] += 1
        
        # Calculer le temps depuis le dernier événement
        time_since_last = calculate_time_since_last_event(region, timestamp_ms)
        
        # Classification magnitude
        if magnitude < 2.0:
            severity = 'micro'
            risk_level = 0
        elif magnitude < 4.0:
            severity = 'minor'
            risk_level = 1
        elif magnitude < 5.0:
            severity = 'light'
            risk_level = 2
        elif magnitude < 6.0:
            severity = 'moderate'
            risk_level = 3
        elif magnitude < 7.0:
            severity = 'strong'
            risk_level = 4
        else:
            severity = 'major'
            risk_level = 5
        
        # Classification profondeur (avec profondeur corrigée)
        if depth_km < 70:
            depth_category = 'shallow'
            depth_risk = 'high'
        elif depth_km < 300:
            depth_category = 'intermediate'
            depth_risk = 'medium'
        else:
            depth_category = 'deep'
            depth_risk = 'low'
        
        # Normalisation coordonnées
        lat_normalized = (latitude + 90) / 180
        lon_normalized = (longitude + 180) / 360
        
        # Valeurs optionnelles
        felt_reports = safe_int(raw_data.get('felt_reports'), 0)
        tsunami = safe_int(raw_data.get('tsunami'), 0)
        significance = safe_int(raw_data.get('significance'), 0)
        alert_level = raw_data.get('alert_level') or 'none'
        cdi = raw_data.get('cdi')
        mmi = raw_data.get('mmi')
        
        # Danger score amélioré
        danger_score = (
            risk_level * 0.4 +  # Magnitude
            (1 if depth_category == 'shallow' else 0.5 if depth_category == 'intermediate' else 0) * 0.3 +  # Profondeur
            (1 if tsunami == 1 else 0) * 0.2 +  # Tsunami
            (min(felt_reports / 1000, 1)) * 0.1  # Population
        )
        
        # Population exposure
        if felt_reports > 100:
            population_exposure = 'high'
        elif felt_reports > 10:
            population_exposure = 'medium'
        else:
            population_exposure = 'low'
        
        # Quality assessment
        has_magnitude = raw_data.get('magnitude') is not None
        has_latitude = raw_data.get('latitude') is not None
        has_longitude = raw_data.get('longitude') is not None
        has_depth = raw_data.get('depth_km') is not None
        data_quality = 'high' if all([has_magnitude, has_latitude, has_longitude, has_depth]) else 'medium'
        
        # 📊 STRUCTURE ML-READY (selon votre demande)
        cleaned = {
            'event_id': raw_data['event_id'],
            'timestamp_utc': dt.isoformat(),
            'latitude': latitude,
            'longitude': longitude,
            'magnitude': magnitude,
            'severity': severity,
            'depth_km': depth_km,  # Profondeur corrigée
            'depth_category': depth_category,
            'risk_level': risk_level,
            'danger_score': round(danger_score, 3),
            'significance': significance,
            'alert_level': alert_level,
            'region': region,
            'time_since_last_event_min': time_since_last,
            'ingestion_timestamp': raw_data.get('ingestion_timestamp', datetime.now().isoformat()),
            'schema_version': '1.0',
            
            # Features additionnelles pour ML
            'network': raw_data.get('network') or 'unknown',
            'source': raw_data.get('source') or 'unknown',
            'timestamp_unix': timestamp_ms,
            'year': dt.year,
            'month': dt.month,
            'day': dt.day,
            'hour': dt.hour,
            'minute': dt.minute,
            'day_of_week': dt.weekday(),
            'day_of_year': dt.timetuple().tm_yday,
            'is_weekend': 1 if dt.weekday() >= 5 else 0,
            'is_night': 1 if 22 <= dt.hour or dt.hour <= 6 else 0,
            'magnitude_type': raw_data.get('magnitude_type') or 'unknown',
            'magnitude_normalized': min(magnitude / 10.0, 1.0),
            'latitude_normalized': lat_normalized,
            'longitude_normalized': lon_normalized,
            'location_name': location_str,
            'depth_normalized': min(depth_km / 700.0, 1.0),
            'depth_risk': depth_risk,
            'felt_reports': felt_reports,
            'cdi': cdi,
            'mmi': mmi,
            'tsunami_flag': tsunami,
            'population_exposure': population_exposure,
            'data_quality': data_quality,
            'processing_timestamp': datetime.now().isoformat(),
            'events_in_region': event_count_by_region[region]
        }
        
        return cleaned
        
    except Exception as e:
        print(f"❌ Erreur normalisation: {e}")
        import traceback
        traceback.print_exc()
        return None

# ============= DELIVERY CALLBACK =============
def delivery_report(err, msg):
    """Callback pour le producer"""
    if err is not None:
        print(f"❌ Erreur delivery: {err}")

# ============= MAIN =============
def main():
    print("=" * 70)
    print("🔄 NORMALIZER WITH AVRO - ML-READY VERSION")
    print("=" * 70)
    print(f"📥 Input:  {TOPIC_RAW}")
    print(f"📤 Output: {TOPIC_CLEANED}")
    print(f"📝 Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"👥 Group:  {CONSUMER_GROUP}")
    print("=" * 70)
    
    consumer, string_deserializer, avro_deserializer = create_avro_consumer()
    producer, string_serializer, avro_serializer = create_avro_producer()
    
    processed_count = 0
    error_count = 0
    corrected_depth_count = 0
    start_time = time.time()
    
    print("\n🚀 Traitement des messages Avro...\n")
    
    try:
        while True:
            msg = consumer.poll(timeout=5.0)
            
            if msg is None:
                if processed_count > 0:
                    elapsed = time.time() - start_time
                    if elapsed > 10:
                        print("\n⏸️ Plus de messages depuis 10 secondes")
                        break
                continue
            
            if msg.error():
                print(f"❌ Erreur consumer: {msg.error()}")
                continue
            
            try:
                key = string_deserializer(msg.key()) if msg.key() else None
                raw_data = avro_deserializer(
                    msg.value(),
                    SerializationContext(TOPIC_RAW, MessageField.VALUE)
                )
                
                # Compter les profondeurs négatives avant normalisation
                raw_depth = safe_float(raw_data.get('depth_km'), 0.0)
                if raw_depth < 0:
                    corrected_depth_count += 1
                
                cleaned_data = normalize_earthquake(raw_data)
                
                if cleaned_data:
                    serialized_key = string_serializer(key) if key else None
                    serialized_value = avro_serializer(
                        cleaned_data,
                        SerializationContext(TOPIC_CLEANED, MessageField.VALUE)
                    )
                    
                    producer.produce(
                        topic=TOPIC_CLEANED,
                        key=serialized_key,
                        value=serialized_value,
                        callback=delivery_report
                    )
                    
                    processed_count += 1
                    
                    if processed_count % 10 == 0:
                        elapsed = time.time() - start_time
                        rate = processed_count / elapsed if elapsed > 0 else 0
                        print(f"⏳ {processed_count} traités | {rate:.1f} msg/s | Erreurs: {error_count}")
                    
                    # Afficher les 3 premiers exemples
                    if processed_count <= 3:
                        print(f"\n📋 Exemple #{processed_count}:")
                        print(f"   Event: {cleaned_data['event_id']}")
                        print(f"   Region: {cleaned_data['region']}")
                        print(f"   Magnitude: {cleaned_data['magnitude']} ({cleaned_data['severity']})")
                        print(f"   Depth: {cleaned_data['depth_km']}km ({cleaned_data['depth_category']})")
                        print(f"   Danger Score: {cleaned_data['danger_score']}")
                        if cleaned_data['time_since_last_event_min'] is not None:
                            print(f"   Time since last: {cleaned_data['time_since_last_event_min']} min")
                        print(f"   Quality: {cleaned_data['data_quality']}")
                    
                    producer.poll(0)
                else:
                    error_count += 1
                    
            except Exception as e:
                error_count += 1
                print(f"❌ Erreur traitement message: {e}")
                import traceback
                traceback.print_exc()
    
    except KeyboardInterrupt:
        print("\n\n🛑 Arrêt demandé par l'utilisateur")
    
    finally:
        print("\n📤 Flush des derniers messages...")
        producer.flush(timeout=10)
        
        print("💾 Commit des offsets...")
        consumer.commit()
        
        elapsed = time.time() - start_time
        success_rate = (processed_count / (processed_count + error_count) * 100) if (processed_count + error_count) > 0 else 0
        
        print("\n" + "=" * 70)
        print("📊 RÉSULTATS FINAUX")
        print("=" * 70)
        print(f"✅ Traités avec succès: {processed_count}")
        print(f"❌ Erreurs/Invalides: {error_count}")
        print(f"🔧 Profondeurs corrigées: {corrected_depth_count}")
        print(f"📈 Taux de succès: {success_rate:.1f}%")
        print(f"⏱️ Durée totale: {elapsed:.1f}s")
        if elapsed > 0 and processed_count > 0:
            print(f"🚀 Débit moyen: {processed_count/elapsed:.1f} msg/s")
        print(f"🌍 Régions uniques: {len(last_event_by_region)}")
        print("=" * 70)
        
        consumer.close()
        print("✅ Normalizer arrêté proprement")

if __name__ == "__main__":
    main()