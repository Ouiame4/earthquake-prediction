#!/bin/bash

echo "=========================================="
echo "🧪 TEST COMPLET AVRO + SCHEMA REGISTRY"
echo "=========================================="

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

# 1. Vérifier Kafka (avec kafka-topics au lieu de curl)
echo -e "\n${YELLOW}📋 ÉTAPE 1: Vérification de Kafka${NC}"
if docker exec kafka-earthquake kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; then
    echo -e "${GREEN}✅ Kafka est prêt!${NC}"
else
    echo -e "${RED}❌ Kafka ne répond pas${NC}"
    echo "Démarrez Kafka avec: docker-compose up -d"
    exit 1
fi

# 2. Vérifier Schema Registry
echo -e "\n${YELLOW}📋 ÉTAPE 2: Vérification Schema Registry${NC}"
if curl -s http://localhost:8081/subjects > /dev/null 2>&1; then
    echo -e "${GREEN}✅ Schema Registry est prêt!${NC}"
else
    echo -e "${RED}❌ Schema Registry ne répond pas${NC}"
    exit 1
fi

# 3. Vérifier les topics
echo -e "\n${YELLOW}📋 ÉTAPE 3: Vérification des topics${NC}"
if docker exec kafka-earthquake kafka-topics --bootstrap-server localhost:9092 --list | grep -q "data.raw.earthquakes"; then
    echo -e "${GREEN}✅ Topic data.raw.earthquakes existe${NC}"
else
    echo -e "${YELLOW}⚠️  Topic n'existe pas, création...${NC}"
    python3 create_topics_advanced.py
fi

# 4. Créer le dossier schemas
echo -e "\n${YELLOW}📋 ÉTAPE 4: Création des fichiers schémas${NC}"
mkdir -p schemas

# 5. Créer les schémas Avro
echo -e "\n${YELLOW}Création du schéma RAW...${NC}"
cat > schemas/earthquake_raw.avsc << 'SCHEMA'
{
  "type": "record",
  "name": "EarthquakeRaw",
  "namespace": "com.earthquake.raw",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "timestamp", "type": "long"},
    {"name": "timestamp_updated", "type": ["null", "long"], "default": null},
    {"name": "magnitude", "type": ["null", "double"], "default": null},
    {"name": "magnitude_type", "type": ["null", "string"], "default": null},
    {"name": "location", "type": ["null", "string"], "default": null},
    {"name": "depth_km", "type": ["null", "double"], "default": null},
    {"name": "latitude", "type": ["null", "double"], "default": null},
    {"name": "longitude", "type": ["null", "double"], "default": null},
    {"name": "alert_level", "type": "string", "default": "none"},
    {"name": "felt_reports", "type": ["null", "int"], "default": null},
    {"name": "cdi", "type": ["null", "double"], "default": null},
    {"name": "mmi", "type": ["null", "double"], "default": null},
    {"name": "tsunami", "type": "int", "default": 0},
    {"name": "significance", "type": ["null", "int"], "default": null},
    {"name": "network", "type": ["null", "string"], "default": null},
    {"name": "source", "type": "string"},
    {"name": "ingestion_timestamp", "type": "string"},
    {"name": "api_version", "type": "string"}
  ]
}
SCHEMA

echo -e "${GREEN}✅ Schéma RAW créé${NC}"

echo -e "\n${YELLOW}Création du schéma CLEANED...${NC}"
cat > schemas/earthquake_cleaned.avsc << 'SCHEMA'
{
  "type": "record",
  "name": "EarthquakeCleaned",
  "namespace": "com.earthquake.cleaned",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "timestamp_utc", "type": "string"},
    {"name": "magnitude", "type": "double"},
    {"name": "severity", "type": "string"},
    {"name": "depth_km", "type": "double"},
    {"name": "depth_category", "type": "string"},
    {"name": "latitude", "type": "double"},
    {"name": "longitude", "type": "double"},
    {"name": "danger_score", "type": "double"},
    {"name": "risk_level", "type": "int"},
    {"name": "processing_timestamp", "type": "string"},
    {"name": "schema_version", "type": "string"}
  ]
}
SCHEMA

echo -e "${GREEN}✅ Schéma CLEANED créé${NC}"

# 6. Enregistrer les schémas
echo -e "\n${YELLOW}📋 ÉTAPE 5: Enregistrement des schémas${NC}"
python3 register_schemas.py

# 7. Vérifier les schémas
echo -e "\n${YELLOW}📋 ÉTAPE 6: Vérification des schémas enregistrés${NC}"
echo "Schémas disponibles:"
curl -s http://localhost:8081/subjects | python3 -m json.tool

echo -e "\n=========================================="
echo -e "${GREEN}✅ CONFIGURATION AVRO TERMINÉE!${NC}"
echo "=========================================="
echo ""
echo "🚀 PROCHAINES ÉTAPES:"
echo ""
echo "1️⃣ Lancer le producer Avro:"
echo "   python3 producer_earthquake_avro.py"
echo ""
echo "2️⃣ Dans un autre terminal, vérifier avec:"
echo "   python3 check_cleaned.py"
echo ""
echo "3️⃣ Voir dans Kafka UI:"
echo "   http://localhost:8090"
echo ""
echo "=========================================="
