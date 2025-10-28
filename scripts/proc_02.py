#!/usr/bin/env python3
"""
proc_02.py

Működési elv:
1. Beolvassa az 1 órás és 3 órás aggregált adatokat
2. Párosítja a 3 órás blokkokat az 1 órás adatokkal
3. Súlyokat számol év-nap-órás időtartam relációban (minden órás blokkhoz külön)
4. Az eredményt CSV-ben exportálja

Használat:
    python scripts/proc_02.py

Kimenet: 
    - weights/climatology_weights_hourly.csv

 Szerző: nagabcube (build with agent mode - GitHub Copilot)
 Dátum: 2025-10
"""
import argparse
import pandas as pd
from datetime import timedelta

"""
 Beolvassa az 1 órás és 3 órás aggregált adatokat.
"""
def load_aggregated_data(hourly_file, threehourly_file):
   
    print(f"Beolvasom: {hourly_file}")
    hourly = pd.read_csv(hourly_file)
    hourly['time'] = pd.to_datetime(hourly['time'])
    hourly = hourly[hourly['pr'] > 0].copy()  # Csak pozitív értékek
    print(f"  - {len(hourly)} órás adat > 0")
    
    print(f"Beolvasom: {threehourly_file}")
    threehourly = pd.read_csv(threehourly_file)
    threehourly['time'] = pd.to_datetime(threehourly['time'])
    threehourly = threehourly[threehourly['pr'] > 0].copy()  # Csak pozitív értékek
    print(f"  - {len(threehourly)} háromórás adat > 0")
    
    return hourly, threehourly

"""
 Párosítja az órás adatokat a 3 órás blokkokhoz, kiegészítve év-specifikus információkkal.
    
 A 3 órás blokkok időbélyegei a blokk kezdetét jelentik.
 Pl. 06:00 -> 06:00, 07:00, 08:00 órás adatok
"""
def match_hourly_to_3hourly(hourly, threehourly):

    weights_data = []
    
    for _, row_3h in threehourly.iterrows():
        block_start = row_3h['time']
        block_total = row_3h['pr']
        
        # A 3 órás blokkhoz tartozó 3 óra
        hours_in_block = [
            block_start,
            block_start + timedelta(hours=1), 
            block_start + timedelta(hours=2)
        ]
        
        hourly_values = []
        for i, hour_time in enumerate(hours_in_block):
            # Keresem az órás adatot ehhez az időponthoz
            hourly_match = hourly[hourly['time'] == hour_time]
            
            if len(hourly_match) > 0:
                hourly_value = hourly_match.iloc[0]['pr']
            else:
                hourly_value = 0.0  # Ha nincs adat, 0
                
            hourly_values.append(hourly_value)
        
        # Ellenőrzés: az órás összeg megközelítőleg egyenlő a 3 órással?
        hourly_sum = sum(hourly_values)
        if hourly_sum > 0:  # Csak ha van csapadék
            # Súlyok számítása
            for i, hourly_val in enumerate(hourly_values):
                weight = hourly_val / hourly_sum if hourly_sum > 0 else 0.0
                
                # a dataframe összeállítása
                year = block_start.year
                month = block_start.month
                day = block_start.day
                hour = block_start.hour
                
                weights_data.append({
                    'datetime': block_start,
                    'year': year,
                    'month': month,
                    'day': day,
                    'hour': hour,
                    'year_month_day_hour': f"{year}-{month:02d}-{day:02d} {block_start.hour + i:02d}:00",   
                    'hour_in_3h_block': i,  # 0, 1, 2
                    'hourly_pr': hourly_val,
                    'block_total_pr': hourly_sum,
                    'threehourly_pr': block_total,
                    'weight': weight
                })
    
    return pd.DataFrame(weights_data)

"""
 Aggregálja a súlyokat napi szinten órás időtartamra (year_month_day_hour + hour_in_3h_block).
 Mindegyik órás időközre külön súlyokat számol.
"""
def aggregate_weights(weights_df):
    
    # Csoportosítás és átlagolás
    aggregated = weights_df.groupby(['year_month_day_hour', 'year', 'month', 'day', 'hour', 'hour_in_3h_block']).agg({
        'weight': ['mean', 'std', 'count'],
        'hourly_pr': 'sum',
        'block_total_pr': 'sum'
    }).round(4)
    
    # Oszlop nevek egyszerűsítése
    aggregated.columns = ['weight_mean', 'weight_std', 'count', 'total_hourly_pr', 'total_block_pr']
    aggregated = aggregated.reset_index()
    
    # A fő súly oszlop
    aggregated['weight'] = aggregated['weight_mean']
    
    return aggregated

"""
 Normalizálja a súlyokat, hogy mindegyik 3 órás időtartamban a 3 óra összege = 1.0 legyen.
"""
def normalize_weights(weights_df):

    normalized_weights = []
    
    unique_hours = weights_df['year_month_day_hour'].unique()
    
    for hour in unique_hours:
        hour_data = weights_df[weights_df['year_month_day_hour'] == hour].copy()
        
        if len(hour_data) > 0:
            # Csoportosítás 3 órás időtartam szerint
            for hour_in_block in [0, 1, 2]:
                block_data = hour_data[hour_data['hour_in_3h_block'] == hour_in_block].copy()
                
                if len(block_data) > 0:
                    total_weight = block_data['weight'].sum()
                    
                    if total_weight > 0:
                        block_data['weight'] = block_data['weight'] / total_weight
                    
                    normalized_weights.append(block_data)
        else:
            # Ha nincs adat ehhez a naphoz, egyenletes eloszlást alkalmazunk
            year, month, day_num = day_num.split('-')
            for hour_in_block in [0, 1, 2]:
                normalized_weights.append(pd.DataFrame({
                    'year_month_day_hour': [hour],
                    'year': [int(year)],
                    'month': [int(month)],
                    'day': [int(day_num)],
                    'hour': [int(hour.split(' ')[1].split(':')[0])],
                    'hour_in_3h_block': [hour_in_block], 
                    'weight': [1.0/3.0],
                    'weight_mean': [1.0/3.0],
                    'total_hourly_pr': [0.0],
                    'total_block_pr': [0.0]
                }))
    
    return pd.concat(normalized_weights, ignore_index=True)

"""
 Menti a klimatológiai súlyokat CSV formátumban
"""
def save_csv(weights_df, output_file):
    
    # Csak a szükséges oszlopok
    weights_df['weight'] = weights_df['weight_mean'] # a fő súly oszlop
    output_df = weights_df[['year_month_day_hour', 'year', 'month', 'day', 'hour', 'hour_in_3h_block', 'weight']].copy()
    
    # Rendezés
    output_df = output_df.sort_values(['year', 'month', 'day', 'hour'])
    
    # Mentés
    output_df.to_csv(output_file, index=False, float_format='%.4f')
    
    print(f"\nÓrai klimatológiai súlyok mentve: {output_file}")
    print(f"Sorok száma: {len(output_df)}")
    print(f"Különböző órák: {output_df['year_month_day_hour'].nunique()}")

"""
 Főmodul
"""
def main():
    parser = argparse.ArgumentParser(description='Klimatológiai súlyok létrehozása órás időskálán')
    parser.add_argument('--hourly-file', 
                       default='results/pr_1hourly_aggregated.csv',
                       help='1 órás aggregált CSV fájl')
    parser.add_argument('--threehourly-file',
                       default='results/pr_3hourly_aggregated.csv', 
                       help='3 órás aggregált CSV fájl')
    parser.add_argument('--output-hourly',
                       default='weights/climatology_weights_hourly.csv',
                       help='Kimeneti órás klimatológiai súlyok CSV')
    parser.add_argument('--detailed-output-hourly',
                       default='weights/climatology_weights_hourly_detailed.csv',
                       help='Részletes órai kimeneti fájl')
    
    args = parser.parse_args()
    
    print("\n\n=== Klimatológiai súlyok létrehozása ===")
    
    # 1. Adatok beolvasása
    hourly, threehourly = load_aggregated_data(args.hourly_file, args.threehourly_file)
    
    # 2. Párosítás és súlyok számítása
    print("\n=== Órás és 3-órás adatok párosítása ===")
    weights_raw = match_hourly_to_3hourly(hourly, threehourly)
    print(f"Párosított esetek: {len(weights_raw)}")
    
    # Az adatperiódus megjelenítése
    print(f"Adatperiódus: {weights_raw['datetime'].min()} - {weights_raw['datetime'].max()}")
    print(f"Évek: {sorted(weights_raw['year'].unique())}")
    print(f"Különböző órák: {weights_raw['year_month_day_hour'].nunique()}")
      
    # 3. Aggregálás 
    print("\n=== Súlyok aggregálása órai relációban ===")
    weights_agg_hourly = aggregate_weights(weights_raw)
    print(f"Aggregált súlyok (órai): {len(weights_agg_hourly)} sor")
    
    # 4. Normalizálások
    print("\n=== Súlyok normalizálása ===")
    weights_final_hourly = normalize_weights(weights_agg_hourly)

    # 5. Mentések
    print("\n=== Mentés ===")
    save_csv(weights_final_hourly, args.output_hourly)
    # Részletes verziók is
    weights_final_hourly.to_csv(args.detailed_output_hourly, index=False, float_format='%.4f')
    print(f"Részletes órai verzió: {args.detailed_output_hourly}")

    print("=" * 10)
    print(f"✅ Kész!")
    print("=" * 10)

if __name__ == '__main__':
    main()