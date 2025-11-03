# 🎲 Sztochasztikus Csapadék Disaggregáció

## 📋 Mi Ez és Miért Kell?

A **sztochasztikus disaggregáció** egy meteorológiai módszer, amely véletlenszerűen felbontja a jövőbeli 3-órás csapadékadatokat órás értékekre, a múltbeli klimatológiai mintázatok alapján.

### 🎯 Alapelv

**Input:** 
- Jövőbeli 3-órás csapadékértékek (2026-2100, `output.db`)
- Múltbeli klimatológiai súlyok (2021-2025, év-hónap-óra/év-hónap)

**Folyamat:**
1. **Időszak azonosítás:** Melyik órára/napra/hónapba esik a jövőbeli adat?
2. **Véletlenszerű választás:** Melyik múltbeli évből vegyük a súlyokat?
3. **Disaggregáció:** 3-órás → 3×1-órás értékek

**Output:** 
- Órás csapadékértékek, amelyek megtartják a 3-órás összeget
- Meteorológiai realizmust biztosító változékonyság

## 🔬 Példa a Véletlenszerűségre

### Konkrét Eset: 2026. január 15., 0.5 mm csapadék

**Rendelkezésre álló múltbeli január súlyok:**
```
2023-01: [0.399, 0.255, 0.346]  →  [0.199, 0.127, 0.173] mm
2024-01: [0.348, 0.262, 0.390]  →  [0.174, 0.131, 0.195] mm  
2025-01: [0.287, 0.356, 0.357]  →  [0.144, 0.178, 0.179] mm
```

**Minden futtatáskor más eredmény:**
- A script **véletlenszerűen** választ a 3 lehetséges múltbeli január közül
- Ugyanaz a 0.5 mm háromféleképpen oszlik meg óránként
- Az összeg mindig 0.5 mm marad ✅

### 📊 Statisztikai Változékonyság

15 véletlenszerű választás alapján:
```
Órás csapadék átlag: [0.178, 0.139, 0.183] mm
Órás csapadék szórás: [0.021, 0.020, 0.010] mm  
Variációs koefficiens: [0.116, 0.141, 0.055]
```

## 🛠️ Implementáció

### Létrehozott szkriptek

1. **`ncloader.py`** - GERICS adatok adatbázisba (SQLite) töltése, dátum standardizálások, hőmérséleti adatok (K->°C), csapadék adatok (mm/s->mm/h) átalakítása
2. **`prcalc_01.py`** - Múltbeli csapadék adatok 1 órás aggregálása CSV fájlból
3. **`prcalc_02.py`** - Klimatológiai súlyok létrehozása órás időskálán
4. **`prcalc_03.py`** - Fő disaggregációs engine

## 🎯 Meteorológiai Jelentőség

### ✅ Miért Helyes Ez a Módszer?

1. **Klimatológiai Alapozás**
   - Múltbeli valós mintázatok használata
   - Szezonális variabilitás figyelembevétele
   - Évjáratok közötti különbségek

2. **Sztochasztikus Realizmus**
   - Véletlenszerűség → természetes változékonyság
   - Nem determinisztikus → több lehetséges kimenet
   - Ensemble modellezéshez alkalmas

3. **Konzisztencia**
   - 3-órás összegek megmaradnak
   - Numerikus stabilitás
   - Fizikai értelemben helyes

4. **Flexibilitás**
   - Év-hónap-óra: Finomabb felbontás
   - Év-hónap: Robosztusabb statisztika
   - Testreszabható random seed

## 🔧 Szakmai Paraméterek

### Random Seed Kezelés
```python
# Reprodukálható eredményekhez
--random-seed 42

# Minden rekordhoz különböző seed
random_seed = base_seed + record_index
```

## 🏆 Összefoglalás

✅ **Véletlenszerűség:** Minden 3-órás értékhez más órás eloszlás  
✅ **Klimatológiai alap:** Múltbeli valós mintázatok  
✅ **Időszak érzékenység:** Év-hónap-óra/év-hónap relációk  
✅ **Konzisztencia:** 3-órás összegek megmaradnak  
✅ **Meteorológiai realizmus:** WMO szabványok szerinti módszer  

# Potenciális evapotranspiráció (PET) számítása HEC-HMS hidrológiai modellezéshez.

## Adatok

- **Forrás**: output.db SQLite adatbázis
- **Táblák**: `tas` (hőmérséklet), `rsds` (sugárzás)
- **Időszak**: 2026-01-01 - 2100-12-31 (75 év)
- **Cell-ek**: 4 db
- **Összesen**: 109,572 napi érték

## PET Számítás

- **Módszer**: Priestley-Taylor egyenlet
- **Formula**: PET = α × (Δ/(Δ+γ)) × Rn
- **Paraméterek**:
  - α = 1.26 (Priestley-Taylor koefficiens)
  - γ = 0.65 hPa/°C (pszichrometrikus konstans)
  - Δ = telítési páranyomás görbe meredeksége
  - Rn = nettó sugárzás [MJ/m²/nap]

## Eredmények

- **Átlagos éves PET**: ~2,750 mm/év
- **Téli minimum**: 0.6-0.9 mm/nap (január, december)
- **Nyári maximum**: 15-16 mm/nap (június, július)
- **Napi tartomány**: 0.02-27 mm/nap

## Fájlok

### Alapfájlok

- `petcalc_01.py` - Alap PET kalkulátor

### Eredmény fájlok

- `pet_cell_[ID]_hec.dss` - HEC-HMS importálható formátum

### Függőségek

- Python 3.12+
- sqlite3 (beépített)
- math (beépített)
- pandas (dataframe kezelés)
- datetime (beépített)
- hecdss (DSS export)

### Futtatás

```bash
# Alap PET számítás
python scripts/petcalc_01.py
```

### Adatbázis struktúra

```sql
-- PR tábla (csapadék)
CREATE TABLE pr (
    time TEXT,      -- 'YYYY-MM-DD HH:MM'  
    cell_id INTEGER,
    pr REAL         -- mm/h
);

-- TAS tábla (hőmérséklet)
CREATE TABLE tas (
    time TEXT,      -- 'YYYY-MM-DD HH:MM'
    cell_id INTEGER,
    tas REAL        -- Celsius
);

-- RSDS tábla (sugárzás)
CREATE TABLE rsds (
    time TEXT,      -- 'YYYY-MM-DD HH:MM'  
    cell_id INTEGER,
    rsds REAL       -- W/m²
);
```
