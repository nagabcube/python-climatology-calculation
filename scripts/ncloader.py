#!/usr/bin/env python3
"""
 ncloader.py

 A parameterként megadott könyvtár összes nc fájlát feldolgozza és SQLite DB-be menti
 Létrehozza:
  - cells táblát (cell_id, lon, lat) - cella középpont geometria
  - pr táblát (time, cell_id, pr) - standardizált időpont (0:00-tól induló), mm/h-ra átváltott csapadékmennyiség a 3 órás blokkokban
  - tas táblát (time, cell_id, tas) - standardizált időpont (0:00-tól induló), Celsius fokra váltott hőmérséklet
  - rsds táblát (time, cell_id, rsds) - standardizált időpont (0:00-tól induló)

 Használat:
    python scripts\ncloader.py --directory-name --db-file (opcionális)

Kimenetek: 
    DB: data/nemti.db

Szerző: nagabcube
Dátum: 2025-10
"""

import os
import argparse
import netCDF4 as nc
import numpy as np
import sqlite3 as sql
from datetime import timedelta

def create_tables(curs):
    # SQLite adatbázis fájl inicializálása
    # csapadék adatok
    curs.execute("DROP TABLE IF EXISTS pr")
    curs.execute("""
    CREATE TABLE pr (
        time TEXT NOT NULL,
        cell_id INTEGER NOT NULL,
        pr REAL NOT NULL,
        PRIMARY KEY (time, cell_id)
    )
    """)
    # besugárzás adatok
    curs.execute("DROP TABLE IF EXISTS rsds")
    curs.execute("""
    CREATE TABLE rsds (
        time TEXT NOT NULL,
        cell_id INTEGER NOT NULL,
        rsds REAL NOT NULL,
        PRIMARY KEY (time, cell_id)
    )
    """)
    # hőmérséklet adatok
    curs.execute("DROP TABLE IF EXISTS tas")
    curs.execute("""
    CREATE TABLE tas (
        time TEXT NOT NULL,
        cell_id INTEGER NOT NULL,
        tas REAL NOT NULL,
        PRIMARY KEY (time, cell_id)
    )
    """)
    # cella koordináták (WGS)
    curs.execute("DROP TABLE IF EXISTS cells")
    curs.execute("""
    CREATE TABLE cells (
        cell_id INTEGER NOT NULL,
        lon DECIMAL(10, 8) NOT NULL,
        lat DECIMAL(10, 8) NOT NULL,
        PRIMARY KEY (cell_id)
    )
    """)


def store_celldata(fn, curs):
    # a szükséges cellák geometriai adatait állítjuk elő
    # a keret koordinátái, amiben keresünk:
    min_lon, max_lon = 19.74, 20.02
    min_lat, max_lat = 47.98, 48.18 # ezek a nemti sub-basin keretét definálják

    dataset = nc.Dataset(fn, 'r')
    lats = dataset.variables['lat'][:]
    lons = dataset.variables['lon'][:]

    mask = (lats >= min_lat) & (lats <= max_lat) & (lons >= min_lon) & (lons <= max_lon)
    ny, nx = np.where(mask)

    # ny: sor index, nx: oszlop index
    for sor, osz in zip(ny, nx):
        centroid_lat = lats[sor, osz]
        centroid_lon = lons[sor, osz]
        # figyelem: a sorok számozása matematikai koordináta rendszerű és 
        # NEM raszter koordináta rendszer (nem a bal felső pozicióban van a "0"...)
        # vagyis a helyes a sorok számából (412) ki kell vonni a sor indexet,
        # de mint hogy nincs 0.sor, ezért a "helyes" sor-index: 412 - sor + 1, ami 411 - sor...
        sor_idx = 411 - sor
        # a megfelelő sor képzése után a cell_id: (nx: rlon - 1000-es szorzó + ny: rlat)
        id = (osz * 1000) + sor_idx
        curs.execute(f"INSERT INTO cells (cell_id, lon, lat) VALUES ({id}, {centroid_lon}, {centroid_lat});")

    dataset.close()


def time_standardizer(timestamp):
    # átalakítja az időpontot standard időpontra (01:30-24:30 -> 00:00-23:00)
    hour = timestamp.hour
    if hour == 0:
        corrected_hour = 23
        corrected_date = timestamp - timedelta(days=1)
    else:
        corrected_hour = hour - 1
        corrected_date = timestamp
    return corrected_date.replace(hour=corrected_hour, minute=0)


# NetCDF fájl feldolgozása
def read_netcdf(nc_file, ds_name, cell_id, cur):
    dataset = nc.Dataset(nc_file, 'r')
    time = dataset.variables['time'][:]

    # cell_id-ből sor, oszlop indexek képzése
    si = cell_id%1000
    oi = cell_id//1000
      
    # Csatlakozás az adatbázishoz
    try:
        # minden időpont feldolgozása az nc fájlból
        print(f" - A {si} sor- és {oi} oszlopindexű cella adatait töltöm...")    
        for t in range(len(time)):
            # a meterológiai adat
            value = dataset.variables[ds_name][t]
            if ds_name == 'pr':
                data = (value[si][oi]) * 3600    # csapadék adatok átváltása mm/h mértékegységre
            elif ds_name == 'tas':
                data = (value[si][oi]) - 273.15  # hőmérséklet adatok átváltása Kelvinről Celsius fokra
            else:
                data = value[si][oi]
            # bohóckodás a GERICS idő átalakításával
            time = dataset.variables['time'][:]
            time_units = dataset.variables['time'].units
            time_calendar = dataset.variables['time'].calendar if 'calendar' in dataset.variables['time'].ncattrs() else 'standard'
            date = nc.num2date(time[t], units=time_units, calendar=time_calendar)
            isodate = time_standardizer(date).strftime("%Y-%m-%d %H:%M")
            cur.execute(f"INSERT INTO {ds_name} (time, cell_id, {ds_name}) VALUES ('{isodate}', {cell_id}, {data});")

    finally:
        cur.execute(f"SELECT count(*) FROM {ds_name}")
        row = cur.fetchone()[0]
        print(f"  * A(z) {ds_name} táblába eddig {row} sort szúrtam be...")

    dataset.close()

#
# főprogram
#
def main():
    parser = argparse.ArgumentParser(description='GERICS netCDF meterológiai adatok (pr/tas/rsds) betöltése adatbázisba')
    parser.add_argument('--directory-name',
                        type=str, 
                        #required=True,
                        default='../ncfiles',
                        help='A netCDF fájlok könytárának útvonala/neve')
    parser.add_argument('--db-path',
                        default='data/nemti.db',
                        help='Az SQLite fájl útvonala/neve')
    args = parser.parse_args()

    # paraméterek
    dir_path = args.directory_name
    sql_file = args.db_path

    print(f"\n=== A GERICS nc fájlok feldolgozása megkezdődött ===")

    conn = sql.connect(sql_file)
    cursor = conn.cursor()

    create_tables(cursor)
    #
    #  elsőnek előállítjuk a cella adatok tábláját
    #
    cellfile = os.path.join(dir_path, os.listdir(dir_path)[0])
    store_celldata(cellfile, cursor)
    # ellenőrzés
    print("\nAz előállt celláink:")
    cursor.execute("SELECT cell_id, lon, lat FROM cells LIMIT 8")
    CELLS = []

    for row in cursor.fetchall():
        print(f"  {row}")
        CELLS.append(row[0])

    #
    # az .nc fájlok feldolgozásának ciklusa
    #
    for filename in os.listdir(dir_path):
        if filename.endswith(".nc"):
            file_path = os.path.join(dir_path, filename)
            dsn = filename.split("_")[0]  # datasource_name (pr/rsds/tas) a fájlnév alapján
            
            print(f"\nA(z) {filename} fájlt beolvastam...")
            # minden cellára lefuttatjuk az olvasó függvényt
            for cell in CELLS:
                read_netcdf(file_path, dsn, cell, cursor)

    cursor.close()
    conn.commit()
    conn.close()

    print("=" * 50)
    print(f"✅ A feldolgozás befejeződött: adatbázis feltöltve!")
    print("=" * 50)

if __name__ == '__main__':
    main()