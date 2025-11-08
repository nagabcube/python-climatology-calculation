#!/usr/bin/env python3
"""
petcalc_01.py

- PET számítása GERICS TAS és RSDS adatokból (SQLite adatbázisból)
  Priestley-Taylor módszerrel, ill. HEC-DSS kimenetek előállítása 

Előfeltételek:
- a TAS értékek már °C fokban (nc_loader.py elvégezte a K-°C átváltást)
- a TAS és RSDS időpontok standardizálása is megtörtént korábban

Használat:
    python scripts/petcalc_01.py --db-path

Kimenet: 
    - results/pet_cell_xxxxxx_hec.dss (ahol xxxxxx a cella azonosító)

Szerző: nagabcube (build with agent mode - GitHub Copilot)
Dátum: 2025-10
"""

import os
import argparse
import math
import sqlite3 as sql
import pandas as pd

class PETCalculator:
    """ PET számítás Priestley-Taylor módszerrel """
    
    def __init__(self):
        self.alpha = 1.26  # Priestley-Taylor koefficients
        self.gamma = 0.65  # pszichrometrikus konstans [hPa/°C]
    
    def magnus_formula(self, temp_celsius):
        """ Magnus formula - telítési páranyomás számítása [hPa] """
        return 6.108 * math.exp((17.27 * temp_celsius) / (temp_celsius + 237.3))
    
    def delta_calculation(self, temp_celsius):
        """ Telítési páranyomás görbe meredeksége [hPa/°C] """
        e_star = self.magnus_formula(temp_celsius)
        return (4098 * e_star) / ((temp_celsius + 237.3) ** 2)
    
    def priestley_taylor_method(self, temp_celsius, radiation_wm2):
        """ 
        Priestley-Taylor módszer PET számításhoz
        
        Args:
            temp_celsius: hőmérséklet [°C]
            radiation_wm2: napsugárzás [W/m²]
        
        Returns:
            pet: potenciális evapotranspiráció [mm/nap]
        """
        delta = self.delta_calculation(temp_celsius)
        
        # Sugárzás átváltása MJ/m²/nap-ra
        # W/m² -> MJ/m²/nap: * 0.0864 (86400 sec/day / 1000000 J/MJ)
        rn = radiation_wm2 * 0.0864
        
        # PET számítása [mm/nap]
        pet = self.alpha * (delta / (delta + self.gamma)) * rn
        
        return pet

class HecDSSExporter:
    """ HEC-DSS export hecdss könyvtárral """
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.pet_calc = PETCalculator()
    
    def get_pet_dataframe(self, cell_id):
        """ PET adatok pandas DataFrame-ben """
        
        conn = sql.connect(self.db_path)
        
        query = """
        SELECT 
            substr(t.time, 1, 10) as date,
            AVG(t.tas) as avg_temp,
            AVG(r.rsds) as avg_radiation
        FROM tas t
        JOIN rsds r ON t.time = r.time AND t.cell_id = r.cell_id
        WHERE t.cell_id = ?
        GROUP BY substr(t.time, 1, 10)
        ORDER BY date
        """
        
        # DataFrame létrehozása
        df = pd.read_sql_query(query, conn, params=(cell_id,))
        conn.close()
        
        # PET számítás
        df['pet'] = df.apply(
            lambda row: self.pet_calc.priestley_taylor_method(
                row['avg_temp'], 
                row['avg_radiation']
            ), 
            axis=1
        )
        
        # Dátum konvertálása
        df['datetime'] = pd.to_datetime(df['date'])
        return df[['datetime', 'pet']]
    
    def export_to_dss(self, cell_id, output_dir):
        """ DSS fájl exportálás hecdss könyvtárral """
        
        try:
            from hecdss.hecdss import HecDss
            from hecdss.hecdss import RegularTimeSeries
            
            # PET adatok lekérése DataFrame-ben
            df = self.get_pet_dataframe(cell_id)
            #df.to_csv(f'pet_{cell_id}_dss.txt')
            
            if df.empty:
                print(f"Nincs adat cell_id {cell_id}-hez")
                return None
            
            # DSS fájl útvonal
            if not os.path.exists(output_dir):
                os.makedirs(output_dir)
            
            dss_file = os.path.join(output_dir, f"pet_cell_{cell_id}_hec.dss")
            
            # DSS pathname (HEC 7.0 formátum)
            pathname = f"/BASIN/CELL_{cell_id}/PET//1DAY/FORECAST/"
            
            print(f"Cell {cell_id} DSS export...")
            print(f"  Pathname: {pathname}")
            print(f"  Records: {len(df)}")
            print(f"  Period: {df['datetime'].min()} - {df['datetime'].max()}")
            
            # DataFrame előkészítése DSS íráshoz
            # Index beállítása dátumra és egy oszlop az adatokkal
            dss_df = df.set_index('datetime')[['pet']]
            dss_df.units = "MM"
            dss_df.data_type = "INST-VAL"
            
            # HEC-DSS írás
            dss = HecDss(dss_file)

            tsc = RegularTimeSeries()
            tsc.id = pathname
            tsc.values = dss_df[dss_df.columns[0]].values.astype(float)
            tsc.times = dss_df.index.tolist()
            tsc.units = dss_df.units
            tsc.data_type = dss_df.data_type
            dss.put(tsc)
            dss.close()

            print(f"✅ DSS fájl létrehozva: {dss_file}")
            return dss_file

        except ImportError as e:
            print(f"❌ hecdss könyvtár nem elérhető: {e}")
            return None
        except Exception as e:
            print(f"❌ Hiba DSS exportáláskor: {e}")
            return None
    
    def export_all_cells(self, output_dir="results"):
        """ Minden cell exportálása """
        
        # Cell ID-k lekérése
        conn = sql.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT cell_id FROM tas ORDER BY cell_id")
        cell_ids = [row[0] for row in cursor.fetchall()]
        conn.close()
        
        print("=== HEC-DSS Export (hecdss könyvtár) ===")
        print(f"Cell ID-k: {cell_ids}")
        
        success_files = []
        
        for cell_id in cell_ids:
            dss_file = self.export_to_dss(cell_id, output_dir)
            if dss_file:
                success_files.append(dss_file)
        
        if success_files:
            print("\n📋 Létrehozott fájlok:")
            for file in success_files:
                size_mb = os.path.getsize(file) / (1024 * 1024)
                print(f"  • {os.path.basename(file)} ({size_mb:.1f} MB)")
       
        return success_files

def main():
    """ Főprogram """
    parser = argparse.ArgumentParser(description='PET számítása GERICS TAS és RSDS adatokból Priestley-Taylor módszerrel')
    parser.add_argument('--db-path',
                        type=str, 
                        #required=True,
                        default='data/basin.db',
                        help='SQLite adatbázis a TAS és RSDS adatokkal')
    args = parser.parse_args()

    exporter = HecDSSExporter(args.db_path)
    
    # Minden cella exportálása
    success_files = exporter.export_all_cells()
    
    if success_files:
        print(f"\n🎉 DSS export befejezve!")
        print(f"   {len(success_files)} fájl készen áll HEC-HMS használatra")
    else:
        print(f"\n❌ DSS export sikertelen")

if __name__ == "__main__":
    main()