#!/usr/bin/env python3
"""
InfluxDB Schema Explorer für Enpal Integration
Dieses Skript hilft dabei, die aktuelle Datenstruktur in der Enpal InfluxDB zu analysieren.
"""

import asyncio
from influxdb_client import InfluxDBClient
import json
from datetime import datetime, timedelta

def explore_influxdb_schema(ip: str, port: int, token: str, org: str = "enpal", bucket: str = "solar"):
    """
    Erforscht die InfluxDB-Schema und gibt alle verfügbaren Messungen und Felder aus.
    """
    print(f"🔍 Verbinde mit InfluxDB: http://{ip}:{port}")
    print(f"📊 Organisation: {org}")
    print(f"🪣 Bucket: {bucket}")
    print("=" * 60)
    
    try:
        # Verbindung zur InfluxDB
        client = InfluxDBClient(url=f'http://{ip}:{port}', token=token, org=org)
        query_api = client.query_api()
        
        # 1. Alle verfügbaren Messungen finden
        print("📋 Suche nach allen verfügbaren Messungen...")
        measurements_query = f'''
        import "influxdata/influxdb/schema"
        schema.measurements(bucket: "{bucket}")
        '''
        
        try:
            measurements_result = query_api.query(measurements_query)
            measurements = []
            for table in measurements_result:
                for record in table.records:
                    measurements.append(record.get_value())
            
            print(f"✅ Gefundene Messungen: {measurements}")
            print()
        except Exception as e:
            print(f"⚠️  Fehler beim Abrufen der Messungen: {e}")
            print("🔄 Versuche alternative Methode...")
            
            # Alternative: Suche nach Messungen in den letzten 24 Stunden
            alt_query = f'''
            from(bucket: "{bucket}")
              |> range(start: -24h)
              |> group(columns: ["_measurement"])
              |> distinct(column: "_measurement")
            '''
            measurements_result = query_api.query(alt_query)
            measurements = []
            for table in measurements_result:
                for record in table.records:
                    measurements.append(record.get_value())
            
            print(f"✅ Gefundene Messungen (Alternative): {measurements}")
            print()
        
        # 2. Für jede Messung alle Felder finden
        schema_data = {}
        
        for measurement in measurements:
            print(f"🔍 Analysiere Messung: {measurement}")
            
            # Suche nach allen Feldern für diese Messung
            fields_query = f'''
            from(bucket: "{bucket}")
              |> range(start: -24h)
              |> filter(fn: (r) => r["_measurement"] == "{measurement}")
              |> group(columns: ["_field"])
              |> distinct(column: "_field")
            '''
            
            try:
                fields_result = query_api.query(fields_query)
                fields = []
                for table in fields_result:
                    for record in table.records:
                        fields.append(record.get_value())
                
                schema_data[measurement] = fields
                print(f"   📊 Felder: {fields}")
                
                # Zeige einige Beispielwerte für jedes Feld
                for field in fields[:3]:  # Nur die ersten 3 Felder als Beispiel
                    sample_query = f'''
                    from(bucket: "{bucket}")
                      |> range(start: -1h)
                      |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                      |> filter(fn: (r) => r["_field"] == "{field}")
                      |> last()
                    '''
                    
                    try:
                        sample_result = query_api.query(sample_query)
                        if sample_result and len(sample_result) > 0 and len(sample_result[0].records) > 0:
                            sample_value = sample_result[0].records[0].get_value()
                            sample_time = sample_result[0].records[0].get_time()
                            print(f"   📈 Beispiel {field}: {sample_value} (um {sample_time})")
                    except Exception as e:
                        print(f"   ⚠️  Fehler beim Abrufen von Beispielwerten für {field}: {e}")
                
            except Exception as e:
                print(f"   ⚠️  Fehler beim Analysieren der Felder für {measurement}: {e}")
                schema_data[measurement] = []
            
            print()
        
        # 3. Speichere die Ergebnisse
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"enpal_schema_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump({
                'timestamp': datetime.now().isoformat(),
                'influxdb_url': f'http://{ip}:{port}',
                'organization': org,
                'bucket': bucket,
                'schema': schema_data
            }, f, indent=2, ensure_ascii=False)
        
        print(f"💾 Schema wurde in {filename} gespeichert")
        
        # 4. Vergleich mit der aktuellen Integration
        print("\n" + "=" * 60)
        print("🔍 VERGLEICH MIT AKTUELLER INTEGRATION")
        print("=" * 60)
        
        current_fields = {
            'inverter': [
                'Power.DC.Total', 'Power.House.Total', 'Voltage.Phase.A', 'Current.Phase.A',
                'Power.AC.Phase.A', 'Voltage.Phase.B', 'Current.Phase.B', 'Power.AC.Phase.B',
                'Voltage.Phase.C', 'Current.Phase.C', 'Power.AC.Phase.C',
                'Power.Battery.Charge.Discharge', 'Energy.Battery.Charge.Level',
                'Energy.Battery.Charge.Day', 'Energy.Battery.Discharge.Day',
                'Energy.Battery.Charge.Total.Unit.1', 'Energy.Battery.Discharge.Total.Unit.1'
            ],
            'system': [
                'Power.External.Total', 'Energy.Consumption.Total.Day',
                'Energy.External.Total.Out.Day', 'Energy.External.Total.In.Day',
                'Energy.Production.Total.Day'
            ],
            'wallbox': [
                'State.Wallbox.Connector.1.Charge', 'Power.Wallbox.Connector.1.Charging',
                'Energy.Wallbox.Connector.1.Charged.Total'
            ]
        }
        
        for measurement, expected_fields in current_fields.items():
            print(f"\n📊 Messung: {measurement}")
            if measurement in schema_data:
                available_fields = schema_data[measurement]
                missing_fields = [field for field in expected_fields if field not in available_fields]
                new_fields = [field for field in available_fields if field not in expected_fields]
                
                print(f"   ✅ Verfügbare Felder: {len(available_fields)}")
                if missing_fields:
                    print(f"   ❌ Fehlende Felder: {missing_fields}")
                if new_fields:
                    print(f"   🆕 Neue Felder: {new_fields}")
            else:
                print(f"   ❌ Messung '{measurement}' nicht gefunden!")
        
        client.close()
        return schema_data
        
    except Exception as e:
        print(f"❌ Fehler bei der Verbindung zur InfluxDB: {e}")
        return None

def main():
    """Hauptfunktion für die Kommandozeile"""
    print("🚀 Enpal InfluxDB Schema Explorer")
    print("=" * 60)
    
    # Konfiguration - bitte anpassen
    ip = input("IP-Adresse der Enpal-Anlage: ").strip()
    port = input("Port (Standard: 8086): ").strip() or "8086"
    token = input("Access Token: ").strip()
    
    if not ip or not token:
        print("❌ IP-Adresse und Token sind erforderlich!")
        return
    
    try:
        port = int(port)
    except ValueError:
        print("❌ Ungültiger Port!")
        return
    
    schema = explore_influxdb_schema(ip, port, token)
    
    if schema:
        print("\n✅ Schema-Analyse abgeschlossen!")
        print("📝 Überprüfen Sie die generierte JSON-Datei für Details.")
    else:
        print("\n❌ Schema-Analyse fehlgeschlagen!")

if __name__ == "__main__":
    main() 