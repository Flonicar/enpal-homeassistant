#!/usr/bin/env python3
"""
Schneller InfluxDB Schema Check für Enpal
Einfache Version für schnelle Überprüfungen
"""

from influxdb_client import InfluxDBClient

def quick_schema_check(ip: str, port: int, token: str, org: str = "enpal", bucket: str = "solar"):
    """
    Schnelle Überprüfung der InfluxDB-Schema
    """
    print(f"🔍 Schnelle Schema-Überprüfung für {ip}:{port}")
    print("=" * 50)
    
    try:
        client = InfluxDBClient(url=f'http://{ip}:{port}', token=token, org=org)
        query_api = client.query_api()
        
        # Suche nach allen Messungen in den letzten 24 Stunden
        query = f'''
        from(bucket: "{bucket}")
          |> range(start: -24h)
          |> group(columns: ["_measurement", "_field"])
          |> distinct(columns: ["_measurement", "_field"])
          |> sort(columns: ["_measurement", "_field"])
        '''
        
        result = query_api.query(query)
        
        measurements = {}
        for table in result:
            for record in table.records:
                measurement = record.get_value("_measurement")
                field = record.get_value("_field")
                
                if measurement not in measurements:
                    measurements[measurement] = []
                measurements[measurement].append(field)
        
        print("📊 Gefundene Messungen und Felder:")
        print()
        
        for measurement, fields in measurements.items():
            print(f"🔹 {measurement}:")
            for field in sorted(fields):
                print(f"   • {field}")
            print()
        
        # Spezielle Überprüfung für bekannte Felder
        print("🔍 Überprüfung bekannter Felder:")
        known_fields = {
            'inverter': ['Power.DC.Total', 'Power.House.Total', 'Power.Battery.Charge.Discharge'],
            'system': ['Power.External.Total', 'Energy.Production.Total.Day'],
            'wallbox': ['State.Wallbox.Connector.1.Charge']
        }
        
        for measurement, expected_fields in known_fields.items():
            print(f"\n📋 {measurement}:")
            if measurement in measurements:
                available = measurements[measurement]
                for field in expected_fields:
                    status = "✅" if field in available else "❌"
                    print(f"   {status} {field}")
            else:
                print(f"   ❌ Messung '{measurement}' nicht gefunden")
        
        client.close()
        return measurements
        
    except Exception as e:
        print(f"❌ Fehler: {e}")
        return None

# Beispiel für direkten Aufruf
if __name__ == "__main__":
    # Hier Ihre Daten eingeben:
    IP = "192.168.1.100"  # Ihre Enpal IP
    PORT = 8086           # Standard InfluxDB Port
    TOKEN = "your_token_here"  # Ihr Access Token
    
    quick_schema_check(IP, PORT, TOKEN) 