# Projekt weather_luigi
Projekt ma zademonstrować wykorzystanie frameworku Luigi z wykorzystaniem checksum do identyfikacji zmian w plikach.

```
repo_root/
│
├── download_api.py
├── pipeline.py
└── main.py
```
** download_api.py **
Odpowiada za:
•	pobranie danych z API, 
•	policzenie checksum, 
•	utworzenie katalogu dla konkretnego przebiegu, 
•	zapis surowych danych do raw_input.json, 
•	zapis metadanych przebiegu do run_metadata.json.
** pipeline.py **
Zawiera taski Luigi:
•	ExtractTask 
•	TransformTask 
•	FinalTask 
Pipeline działa dla konkretnego checksum, który identyfikuje jedną wersję danych wejściowych.
** main.py **
To punkt wejścia programu. Odpowiada za:
•	pobranie danych, 
•	policzenie checksum, 
•	sprawdzenie rejestru przetworzonych checksum, 
•	decyzję: 
o	uruchomić Luigi, 
o	albo pominąć pipeline.

## Kontakt

Projekt na licencji otwartej.  
Zapraszam do współpracy:  
- [LinkedIn: Michał Jaros](https://www.linkedin.com/in/michał-jaros-88572821a/)  
- E-mail: michal.marek.jaros@gmail.com
