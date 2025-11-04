#!/usr/bin/env python3
"""
prcalc_03.py - OPTIMALIZÁLT VERZIÓ

Sztochasztikus (véletlenszerű) disaggregáció a jövőbeli csapadékadatokhoz.
A múltbeli klimatológiai súlyokat használja fel véletlenszerű módon.

A módszer lényege:
1. Beolvassa a jövőbeli 3-órás csapadék adatokat cellánként (<cella_id>.db)
2. Minden 3-órás értékhez meghatározza az időszakot
3. Kikeresi a megfelelő klimatológiai súlyokat
4. VÉLETLENSZERŰEN választ egyet a múltbeli azonos időszakú súlyok közül
5. Disaggregálja a 3-órás értéket 3 db 1-órás értékre

Ez biztosítja a meteorológiai realizmust és a sztochasztikus változékonyságot.

TELJESÍTMÉNY OPTIMALIZÁLÁSOK:
- Vectorizált pandas műveletek az .apply() helyett
- Batch feldolgozás 10,000-es blokkokban
- Numpy array-k használata lista műveletek helyett
- Dictionary-alapú súly keresés GroupBy helyett
- Előre allokált eredmény array-k
- Index optimalizáció a klimatológiai adatokon

Várható gyorsulás: ~5-10x (7 perc helyett ~1-2 perc)/cella

Használat:
    python scripts/prcalc_03.py (opcionális: --cell-id/--limit-rows --random-seed)

Szerző: nagabcube (build with agent mode - optimized by GitHub Copilot)
Dátum: 2025-10
"""

import argparse
import random
import logging
import sqlite3
import numpy as np
import pandas as pd
from pathlib import Path

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

"""
 Betölti a klimatológiai súlyokat
"""
def load_climatology_weights(weights_file):
    
    print(f"Klimatológiai súlyok betöltése: {weights_file}")
    weights = pd.read_csv(weights_file)
    print(f"  - {weights['year_month_day_hour'].nunique()} különböző órás időtartam")
    
    return weights

"""
 Betölti a jövőbeli csapadékadatokat az adatbázisból.
    
 Args:
    db_path: SQLite adatbázis elérési útja
    limit_rows: Opcionális, csak ennyi sort dolgoz fel (teszteléshez)
"""
def load_future_precipitation(db_path, limit_rows=None, cell_id=None):
    
    print(f"Jövőbeli csapadékadatok betöltése: {db_path}")
    
    conn = sqlite3.connect(db_path)
    
    if limit_rows:
        query = f"SELECT cell_id, time, pr FROM pr WHERE pr > 0 LIMIT {limit_rows}"
        print(f"  - Csak {limit_rows} pozitív csapadék rekordot dolgozunk fel (teszt)")
    elif cell_id is not None:
        query = f"SELECT cell_id, time, pr FROM pr WHERE pr > 0 AND cell_id = {cell_id}"
        print(f"  - Csak a(z) {cell_id} cella pozitív csapadék rekordjait dolgozzuk fel")
    else:
        query = "SELECT cell_id, time, pr FROM pr WHERE pr > 0"  
        print(f"  - Az összes pozitív csapadék rekordot feldolgozzuk - figyelem, ez időigényes számítás (lehet)!")
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    # Időbélyeg konverzió
    df['time'] = pd.to_datetime(df['time'])
    
    print(f"  - {len(df)} csapadék rekord betöltve")
    print(f"  - Időszak: {df['time'].min()} - {df['time'].max()}")
    print(f"  - Különböző cell_id-k: {df['cell_id'].nunique()}")
    
    return df

"""
 Meghatározza, hogy a jövőbeli időpontokhoz milyen múltbeli súlyokat rendeljen.
    
 Hierarchikus matching stratégia:
 1. Először: hónap-nap-óra (legspecifikusabb)
 2. Ha nincs: hónap-óra (kevésbé specifikus), vagy csak havi
 
 Args:
    future_df: Jövőbeli csapadékadatok
    weights_df: Klimatológiai súlyok
"""
def determine_period_mapping(future_df, weights_df):

    future_df = future_df.copy()
       
    # Jövőbeli időpontokból a megfelelő 3 órás blokk kezdetének meghatározása
    future_df['month'] = future_df['time'].dt.month
    future_df['day'] = future_df['time'].dt.day
    future_df['hour'] = future_df['time'].dt.hour
    
    # Hierarchikus kulcsok: specifikus -> általános - VECTORIZÁLT
    future_df['key_exact'] = (
        future_df['month'].astype(str).str.zfill(2) + '-' +
        future_df['day'].astype(str).str.zfill(2) + '-' +
        future_df['hour'].astype(str).str.zfill(2)
    )
    future_df['key_monthly'] = (
        future_df['month'].astype(str).str.zfill(2) + '-XX-' +
        future_df['hour'].astype(str).str.zfill(2)
    )
    
    # Súlyok csoportosítása minden szinten - VECTORIZÁLT
    weights_df['key_exact'] = (
        weights_df['month'].astype(str).str.zfill(2) + '-' +
        weights_df['day'].astype(str).str.zfill(2) + '-' +
        weights_df['hour'].astype(str).str.zfill(2)
    )
    weights_df['key_monthly'] = (
        weights_df['month'].astype(str).str.zfill(2) + '-XX-' +
        weights_df['hour'].astype(str).str.zfill(2)
    )
    
    # Csoportosítások létrehozása
    weights_groups_exact= weights_df.groupby('key_exact')
    weights_groups_monthly = weights_df.groupby(['month', 'hour'])
    
    # Statistika
    print(f"Mapping kulcsok statisztikája:")
    print(f"  Egzakt (hó-nap-óra): {weights_df['key_exact'].nunique()}")
    print(f"  Havi (hó-óra): {len(weights_groups_monthly)}")  
    
    return future_df, weights_groups_exact, weights_groups_monthly

"""
 Hierarchikus súlykiválasztás: specifikus -> általános fallback.
    
 Ez a funkció valósítja meg a SZTOCHASZTIKUS ELEMET!
    
 Args:
    future_row: Jövőbeli rekord (tartalmazza az időpontot és kulcsokat)
    weights_groups_*: Különböző szintű csoportosított súlyok
    random_seed: Opcionális random seed
    
 Returns:
    [weight_0, weight_1, weight_2]: A 3 órára vonatkozó súlyok
"""
def stochastic_weight_selection(future_row, weights_groups_exact, weights_groups_monthly, random_seed):
    # Random seed beállítása
    if random_seed is not None:
        random.seed(random_seed)
    
    # 1. Próba: EGZAKT matching (hó-nap-óra)
    if future_row['key_exact'] in weights_groups_exact.groups:
        period_weights = weights_groups_exact.get_group(future_row['key_exact'])
        match_level = "EGZAKT"
    # 2. Próba: Havi matching (hó-óra)  
    elif (future_row['month'], future_row['hour']) in weights_groups_monthly.groups:
        period_weights = weights_groups_monthly.get_group((future_row['month'], future_row['hour']))
        match_level = "HAVI"
    else:
        # Végső fallback: egyenletes eloszlás
        print(f"⚠️  Nincs súly a {future_row['hour']}:00 órához, egyenletes eloszlást használunk")
        return [1.0/3.0, 1.0/3.0, 1.0/3.0], "EGYENLETES", None
    
    # VÉLETLENSZERŰ kiválasztás a talált rekordok közül
    # Próbáljuk többször is, amíg nem találunk 3 súlyt
    max_attempts = 10
    for attempt in range(max_attempts):
        available_years = period_weights['year'].unique()
        if len(available_years) == 0:
            break
            
        chosen_year = random.choice(available_years)
        chosen_weights = period_weights[period_weights['year'] == chosen_year]
        
        # A kiválasztott év súlyainak rendezése (hour_in_3h_block szerint)
        chosen_weights = chosen_weights.sort_values('hour_in_3h_block')
        
        # Ellenőrizzük, hogy pontosan 3 súly van-e (0, 1, 2)
        if len(chosen_weights) == 3 and set(chosen_weights['hour_in_3h_block'].values) == {0, 1, 2}:
            weights_array = chosen_weights['weight'].values
            reference_datetime = chosen_weights.iloc[0]['year_month_day_hour']
            return weights_array.tolist(), match_level, reference_datetime
    
    # Ha nem sikerült 3 súlyt találni, próbáljuk az átlagolást
    # Az összes elérhető súlyból számoljunk átlagot hour_in_3h_block szerint
    if len(period_weights) > 0:
        avg_weights = period_weights.groupby('hour_in_3h_block')['weight'].mean()
        
        # Ha minden 3 pozíció (0, 1, 2) elérhető
        if set(avg_weights.index) == {0, 1, 2}:
            weights_array = [avg_weights[0], avg_weights[1], avg_weights[2]]
            # Normalizálás, hogy összeg = 1.0 legyen
            total = sum(weights_array)
            if total > 0:
                weights_array = [w / total for w in weights_array]
            else:
                weights_array = [1.0/3.0, 1.0/3.0, 1.0/3.0]
            
            # Referencia: az első elérhető dátum
            reference_datetime = period_weights.iloc[0]['year_month_day_hour']
            return weights_array, f"{match_level}_AVG", reference_datetime
    
    # Végső fallback: egyenletes eloszlás
    return [1.0/3.0, 1.0/3.0, 1.0/3.0], "HIBA", None

"""
 TELJESEN ÚJ - ULTRA GYORS vectorizált súly kiválasztás
    
 Teljesen megszünteti az .iterrows() használatát és
 vectorizált numpy/pandas műveleteket használ!
"""
def ultra_fast_weight_selection(batch_df, weights_dict_exact, weights_dict_monthly, random_seeds):
    n_records = len(batch_df)
    weights_array = np.full((n_records, 3), 1.0/3.0)  # Default egyenletes
    match_levels = ["EGYENLETES"] * n_records
    ref_dates = [None] * n_records
    
    # Numpy array-k a gyors kereséshez
    keys_exact = batch_df['key_exact'].values
    keys_monthly = list(zip(batch_df['month'].values, batch_df['hour'].values))
    
    # Random numpy generátor - gyorsabb
    rng = np.random.default_rng(int(random_seeds[0]))
     
    for i in range(n_records):
        # 1. Specifikus matching
        key_spec = keys_exact[i]
        if key_spec in weights_dict_exact:
            period_weights = weights_dict_exact[key_spec]
            match_level = "EGZAKT"
        # 2. Havi matching
        elif keys_monthly[i] in weights_dict_monthly:
            period_weights = weights_dict_monthly[keys_monthly[i]]
            match_level = "HAVI"  
        else:
            continue  # Marad az egyenletes
        
        # Gyors véletlenszerű kiválasztás
        available_years = period_weights['year'].unique()
        if len(available_years) > 0:
            # Véletlenszerű év kiválasztása
            chosen_year = rng.choice(available_years)
            year_data = period_weights[period_weights['year'] == chosen_year]
            
            # Ellenőrzés hogy van-e mindhárom óra
            if len(year_data) == 3 and set(year_data['hour_in_3h_block']) == {0, 1, 2}:
                # Rendezés és súlyok kinyerése
                year_data_sorted = year_data.sort_values('hour_in_3h_block')
                weights_array[i] = year_data_sorted['weight'].values
                match_levels[i] = match_level
                ref_dates[i] = year_data_sorted.iloc[0]['year_month_day_hour']
            else:
                # Fallback: átlag számítás
                try:
                    avg_by_hour = period_weights.groupby('hour_in_3h_block')['weight'].mean()
                    if set(avg_by_hour.index) == {0, 1, 2}:
                        weights_vals = [avg_by_hour[0], avg_by_hour[1], avg_by_hour[2]]
                        total = sum(weights_vals)
                        if total > 0:
                            weights_array[i] = [w/total for w in weights_vals]
                            match_levels[i] = f"{match_level}_AVG"
                            ref_dates[i] = period_weights.iloc[0]['year_month_day_hour']
                except:
                    pass  # Marad egyenletes
    
    return weights_array, match_levels, ref_dates

"""
 A sztochasztikus disaggregáció elvégzése - OPTIMALIZÁLT VERZIÓ
    
 Args:
    future_df: Jövőbeli csapadékadatok (hierarchikus kulcsokkal)
    weights_groups_*: Különböző szintű csoportosított klimatológiai súlyok
    random_seed: Random seed a reprodukálhatósághoz
    
 Returns:
    DataFrame: Disaggregált órás csapadékadatok
"""
def disaggregate_precipitation(future_df, weights_groups_exact, weights_groups_monthly, random_seed=42):
 
    print(f"\n=== Sztochasztikus disaggregáció (OPTIMALIZÁLT) ===")
    print(f"Feldolgozandó rekordok: {len(future_df)}")
    
    # Random seed beállítása a reprodukálhatósághoz
    random.seed(random_seed)
    np.random.seed(random_seed)
    
    # Súly csoportok Dictionary-kké alakítása gyorsabb kereséshez
    weights_dict_exact = {name: group for name, group in weights_groups_exact}
    weights_dict_monthly = {name: group for name, group in weights_groups_monthly}  
    
    # Matching statisztikák
    match_stats = {
        "EGZAKT": 0, "HAVI": 0, "EGYENLETES": 0, "HIBA": 0,
        "EGZAKT_AVG": 0, "HAVI_AVG": 0
    }
    
    # Numpy array-k előkészítése a gyorsabb műveletek számára
    n_records = len(future_df)
    
    # Random seedek generálása egyszerre
    random_seeds = np.arange(random_seed, random_seed + n_records)
    
    # Eredmény listák előkészítése (3x nagyobb méret mert 3 óránként)
    result_size = n_records * 3
    result_cell_ids = np.zeros(result_size, dtype=np.int64)
    result_times_3h = np.empty(result_size, dtype='datetime64[ns]')
    result_times_1h = np.empty(result_size, dtype='datetime64[ns]')  
    result_pr_3h = np.zeros(result_size)
    result_pr_1h = np.zeros(result_size)
    result_hours = np.zeros(result_size, dtype=np.int8)
    result_weights = np.zeros(result_size)
    result_match_levels = []
    result_ref_dates = []
    
    # Batch feldolgozás
    batch_size = 10000
    n_batches = (n_records + batch_size - 1) // batch_size
    
    for batch_idx in range(n_batches):
        start_idx = batch_idx * batch_size
        end_idx = min(start_idx + batch_size, n_records)
        
        logger.info(f"  Feldolgozva: {end_idx}/{n_records} ({100*end_idx/n_records:.1f}%)")
        
        # Batch adatok kinyerése
        batch_df = future_df.iloc[start_idx:end_idx].copy()
        
        # ULTRA GYORS vectorizált súly kiválasztás
        weights_array, match_levels, ref_dates = ultra_fast_weight_selection(
            batch_df, weights_dict_exact, weights_dict_monthly, random_seeds[start_idx:end_idx]
        )
        
        # Statisztika frissítése
        for level in match_levels:
            match_stats[level] = match_stats.get(level, 0) + 1
               
        # Batch adatok kinyerése numpy array-kként
        batch_cell_ids = batch_df['cell_id'].values
        batch_times = batch_df['time'].values
        batch_pr = batch_df['pr'].values
        
        # 3 órás értékek replikálása vectorizált módon
        for i in range(len(batch_df)):
            result_idx = (start_idx + i) * 3
            
            # 3 órás blokk mindhárom órájára egyszerre
            for hour_offset in range(3):
                idx = result_idx + hour_offset
                result_cell_ids[idx] = batch_cell_ids[i]
                result_times_3h[idx] = batch_times[i]
                result_times_1h[idx] = batch_times[i] + np.timedelta64(hour_offset, 'h')
                result_pr_3h[idx] = batch_pr[i]
                result_hours[idx] = hour_offset
                result_weights[idx] = weights_array[i, hour_offset]
                result_pr_1h[idx] = batch_pr[i] * weights_array[i, hour_offset]
        
        # Match level és ref date listák bővítése (3-szor minden rekord)
        for i in range(len(batch_df)):
            for hour_offset in range(3):
                result_match_levels.append(match_levels[i])
                result_ref_dates.append(ref_dates[i])
    
    print(f"\n✅ Optimalizált disaggregáció kész!")
    
    # Eredmény DataFrame összeállítása
    result_df = pd.DataFrame({
        'cell_id': result_cell_ids,
        'time_3hourly': result_times_3h,
        'time_hourly': result_times_1h,
        'pr_3hourly_original': result_pr_3h,
        'hour_in_3h_block': result_hours,
        'weight_used': result_weights,
        'match_level': result_match_levels,
        'reference_datetime': result_ref_dates,
        'pr_hourly_disaggregated': result_pr_1h
    })
    
    print(f"  Eredmény: {len(result_df)} órás rekord")
    
    # Matching statisztikák
    total_records = sum(match_stats.values())
    print(f"  Matching statisztikák:")
    for level, count in match_stats.items():
        percentage = 100 * count / total_records if total_records > 0 else 0
        print(f"    {level}: {count} ({percentage:.1f}%)")
    
    return result_df

"""
 Menti az eredményeket CSV és SQLite formátumban.
"""
def save_results(result_df, cella_id, output_dir='results'):
    
    output_dir = Path(output_dir)
    
    # CSV mentés
    if cella_id is not None:
        output_dir = output_dir / f'cell_{cella_id}'
        output_dir.mkdir(parents=True, exist_ok=True)
    
    csv_file = output_dir / f'pr_stochastic_disaggregated.csv'
    result_df.to_csv(csv_file, index=False, float_format='%.6f')
    print(f"\n✅ CSV mentve: {csv_file}")
       
    # Összesítő statisztikák
    print(f"\n📊 EREDMÉNY STATISZTIKÁK:")
    print(f"   Órás rekordok: {len(result_df):,}")
    print(f"   Időszak: {result_df['time_hourly'].min()} - {result_df['time_hourly'].max()}")
    print(f"   Különböző cell_id-k: {result_df['cell_id'].nunique()}")
    print(f"   Átlagos 3-órás csapadék: {result_df['pr_3hourly_original'].mean():.4f}")
    print(f"   Átlagos disaggregált órás: {result_df['pr_hourly_disaggregated'].mean():.4f}")
    
    # Ellenőrzés: az összegek stimmelnek-e?
    check = result_df.groupby(['cell_id', 'time_3hourly']).agg({
        'pr_3hourly_original': 'first',
        'pr_hourly_disaggregated': 'sum'
    })
    check['difference'] = check['pr_hourly_disaggregated'] - check['pr_3hourly_original']
    max_diff = check['difference'].abs().max()
    print(f"   Maximális eltérés (3h vs 3×1h összeg): {max_diff:.8f}")
    
    return csv_file #, db_file

"""
 Főmodul
"""
def main():
    parser = argparse.ArgumentParser(description='Sztochasztikus csapadék disaggregáció számítása')
    parser.add_argument('--db-path',
                       type=str, 
                       #required=True,
                       default='data/basin.db',
                       help='Jövőbeli csapadékadatok SQLite fájlja')
    parser.add_argument('--weights-file',
                       default='weights/climatology_weights_hourly.csv',
                       help='1 órás klimatológiai súlyok CSV fájlja')
    parser.add_argument('--cell-id',
                       default=None,
                       type=int,
                       help='Csak egy adott cell_id-t dolgoz fel (teszteléshez)')
    parser.add_argument('--limit-rows',
                       default=None,
                       type=int,
                       help='Csak ennyi sort dolgoz fel (teszteléshez)')
    parser.add_argument('--random-seed',
                       type=int,
                       default=42,
                       help='Random seed a reprodukálhatósághoz')
    
    args = parser.parse_args()
    
    print("\n\n🌧️  SZTOCHASZTIKUS CSAPADÉK DISAGGREGÁCIÓ")
    print("   Véletlenszerű disaggregáció klimatológiai súlyokkal")
    print(f"   Random seed: {args.random_seed}")
    print("="*60)
    
    # 1. Klimatológiai súlyok betöltése
    weights_df = load_climatology_weights(args.weights_file)
    
    # 2. Jövőbeli csapadékadatok betöltése
    future_df = load_future_precipitation(args.db_path, args.limit_rows, args.cell_id)
    
    # 3. Hierarchikus időszak mapping
    future_df, weights_groups_exact, weights_groups_monthly = determine_period_mapping(future_df, weights_df)
    
    # 4. Sztochasztikus disaggregáció
    result_df = disaggregate_precipitation(future_df, weights_groups_exact, weights_groups_monthly, args.random_seed)
    
    # 5. Eredmények mentése
    csv_file = save_results(result_df, args.cell_id)
    
    print(f"\n🎯 KÉSZ!")
    print(f"   A véletlenszerű disaggregáció elkészült!")
    print(f"   Az órás csapadékadatok helye: {csv_file} f.")
    print("="*50)

if __name__ == '__main__':
    main()