#!/usr/bin/env python3
"""
Enregistrement des schémas Avro dans le Schema Registry
"""
import requests
import json
import sys
import time

SCHEMA_REGISTRY_URL = "http://localhost:8081"

def wait_for_schema_registry(max_retries=30):
    """Attendre que le Schema Registry soit prêt"""
    print("🔧 Attente du Schema Registry...")
    for i in range(max_retries):
        try:
            response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
            if response.status_code == 200:
                print("✅ Schema Registry est prêt!")
                return True
        except Exception as e:
            if i < max_retries - 1:
                print(f"⏳ Tentative {i+1}/{max_retries}...")
                time.sleep(2)
            else:
                print(f"❌ Schema Registry non accessible: {e}")
                return False
    return False

def register_schema(subject, schema_file):
    """Enregistre un schéma dans le Schema Registry"""
    try:
        # Lire le fichier schéma
        with open(schema_file, 'r') as f:
            schema = json.load(f)
        
        # Préparer la payload
        payload = {
            "schema": json.dumps(schema)
        }
        
        # Enregistrer
        url = f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions"
        response = requests.post(
            url,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            json=payload
        )
        
        if response.status_code in [200, 201]:
            result = response.json()
            print(f"✅ {subject}")
            print(f"   - Schema ID: {result['id']}")
            print(f"   - Version: {result.get('version', 'N/A')}")
            return True
        else:
            print(f"❌ Erreur pour {subject}: {response.status_code}")
            print(f"   {response.text}")
            return False
            
    except FileNotFoundError:
        print(f"❌ Fichier {schema_file} introuvable")
        return False
    except Exception as e:
        print(f"❌ Erreur lors de l'enregistrement de {subject}: {e}")
        return False

def list_schemas():
    """Liste tous les schémas enregistrés"""
    try:
        response = requests.get(f"{SCHEMA_REGISTRY_URL}/subjects")
        if response.status_code == 200:
            subjects = response.json()
            print("\n📋 Schémas enregistrés:")
            for subject in subjects:
                # Obtenir la dernière version
                version_response = requests.get(
                    f"{SCHEMA_REGISTRY_URL}/subjects/{subject}/versions/latest"
                )
                if version_response.status_code == 200:
                    version_data = version_response.json()
                    print(f"  - {subject}")
                    print(f"    ID: {version_data['id']}, Version: {version_data['version']}")
        else:
            print(f"❌ Erreur lors de la liste: {response.status_code}")
    except Exception as e:
        print(f"❌ Erreur: {e}")

def check_compatibility(subject, new_schema_file):
    """Vérifie la compatibilité d'un nouveau schéma"""
    try:
        with open(new_schema_file, 'r') as f:
            schema = json.load(f)
        
        payload = {
            "schema": json.dumps(schema)
        }
        
        url = f"{SCHEMA_REGISTRY_URL}/compatibility/subjects/{subject}/versions/latest"
        response = requests.post(
            url,
            headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
            json=payload
        )
        
        if response.status_code == 200:
            result = response.json()
            if result.get('is_compatible'):
                print(f"✅ {subject}: Compatible")
            else:
                print(f"⚠️ {subject}: Non compatible")
                print(f"   Messages: {result.get('messages', [])}")
        else:
            print(f"❌ Erreur compatibilité {subject}: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Erreur: {e}")

def main():
    print("=" * 70)
    print("📝 ENREGISTREMENT DES SCHÉMAS AVRO")
    print("=" * 70)
    
    if not wait_for_schema_registry():
        sys.exit(1)
    
    # Définir les schémas à enregistrer
    schemas = [
        {
            "subject": "data.raw.earthquakes-value",
            "file": "schemas/earthquake_raw.avsc",
            "description": "Schéma pour données brutes"
        },
        {
            "subject": "data.cleaned.earthquakes-value",
            "file": "schemas/earthquake_cleaned.avsc",
            "description": "Schéma pour données nettoyées"
        }
    ]
    
    print("\n🔄 Enregistrement des schémas...")
    success_count = 0
    
    for schema in schemas:
        print(f"\n📄 {schema['description']}:")
        if register_schema(schema['subject'], schema['file']):
            success_count += 1
    
    print("\n" + "=" * 70)
    print(f"✅ {success_count}/{len(schemas)} schémas enregistrés avec succès")
    print("=" * 70)
    
    # Lister tous les schémas
    list_schemas()
    
    # Vérifier la configuration du Schema Registry
    print("\n🔍 Configuration du Schema Registry:")
    try:
        config_response = requests.get(f"{SCHEMA_REGISTRY_URL}/config")
        if config_response.status_code == 200:
            config = config_response.json()
            print(f"  - Compatibilité: {config.get('compatibilityLevel', 'N/A')}")
    except Exception as e:
        print(f"  ⚠️ Impossible de récupérer la config: {e}")
    
    print("\n📊 URLs utiles:")
    print(f"  - Schema Registry: {SCHEMA_REGISTRY_URL}")
    print(f"  - Liste des sujets: {SCHEMA_REGISTRY_URL}/subjects")
    print(f"  - Kafka UI: http://localhost:8090")
    print("=" * 70)

if __name__ == "__main__":
    main()
