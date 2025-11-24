# PySpark-project
ETL & Machine Learning Pipeline=

```markdown
# PySpark Full ETL + Machine Learning Pipeline – Titanic Survival Prediction  

![PySpark](https://img.shields.io/badge/PySpark-3.5.3-E25A1C?style=for-the-badge&logo=apache-spark&logoColor=white)
![Python](https://img.shields.io/badge/Python-3.11-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Status](https://img.shields.io/badge/Status-Completed-success?style=for-the-badge)

## Apa yang ada di project ini?
- Full ETL pipeline (CSV → Clean → Enrich → Write Parquet + Partition)
- 8 fitur (Title, Deck, FamilySize, IsAlone, dll)
- Training Random Forest (PySpark MLlib) (accuracy ~83–85%)
- Prediksi data baru sebagai testing
  Jack → 0.0 (mati)  
  Rose → 1.0 (selamat)

1. Clone repo
   ```bash
   git clone https://github.com/ronihs/PySpark-project.git
   ```
2. Buka notebook → Run All 

## Hasil Prediksi
```
+----------+-----------------------------------+
|prediction|probability                        |
+----------+-----------------------------------+
|0.0       |[0.74, 0.26] → Jack Dawson (mati)  |
|1.0       |[0.02, 0.98] → Rose (selamat!)     |
+----------+-----------------------------------+
```

## Tech Stack
- Apache Spark 3.5.3 + PySpark
- Python 3.11
- Jupyter Notebook
- Git & GitHub
```
