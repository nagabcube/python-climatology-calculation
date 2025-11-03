#!/usr/bin/env python3
"""
 prcalc_01.py
Múltbeli csapadék adatok 1 órás aggregálása CSV fájlból.

A program feltételezi, hogy 
- a bemeneti CSV fájl pontosvesszővel (;) elválasztott time;pr oszlopokat tartalmaz 
- az időbélyeg (time) formátuma 'YYYY.MM.DD HH:MM' 
- a csapadék adatok (pr) egy óránál sűrűbb értékeket és tizedespont ('.') elválasztót tartalmaznak

Használat:
    python scripts\prcalc_01.py --input-csv data/multbeli_adat.csv

Kimenetek: 
    1 órás: results/pr_1hourly_aggregated.csv
    3 órás: results/pr_3hourly_aggregated.csv

Szerző: nagabcube (build with agent mode - GitHub Copilot)
Dátum: 2025-10
"""

import argparse
import logging
import pandas as pd

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Múltbeli csapadékadatok 1 és 3 órás aggregálása CSV fájlból')
    parser.add_argument('--input-csv',
                        type=str, 
                        #required=True,
                        default='data/multbeli_adat.csv',
                        help='Bemeneti CSV fájl pontosvesszővel elválasztott formátumban')
    parser.add_argument('--output-hourly',
                        default='results/pr_1hourly_aggregated.csv',
                        help='Kimeneti 1 órás aggregált CSV fájl')
    parser.add_argument('--output-threehourly',
                        default='results/pr_3hourly_aggregated.csv',
                        help='Kimeneti 3 órás aggregált CSV fájl')
    args = parser.parse_args()

    print("\nMúltbeli negyedórás csapadék adatok aggregátumainak képzése 1, ill.3 órás blokkokba...\n")
    df = pd.read_csv(args.input_csv, sep=';')

    logger.info("Időbélyeg oszlop konvertálása datetime formátumra (custom formátum megadással)...")
    df['time'] = pd.to_datetime(df['time'], format='%Y.%m.%d %H:%M')

    logger.info("Időbélyeg beállítása indexként")
    df.set_index('time', inplace=True)

    logger.info("Adatok 1 órás aggregálása (összegzés)")
    hourly_data = df.resample('1h').sum()

    hourly_data.to_csv(args.output_hourly)
    logger.info(f"1 órás aggregált adatok mentve: {args.output_hourly}")

    logger.info("Adatok 3 órás aggregálása (összegzés)")   
    threehourly_data = df.resample('3h').sum()

    threehourly_data.to_csv(args.output_threehourly)
    logger.info(f"3 órás aggregált adatok mentve: {args.output_threehourly}")

    print("=" * 50)
    print(f"✅ Az aggregátumok elkészültek (results mappa)")
    print("=" * 50)

if __name__ == '__main__':
    main()