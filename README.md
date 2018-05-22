# BigDataTorlone
Repo per il corso di Big Data di Roma Tre

#### Homework 1
Gli script nella cartella HW1 vanno eseguiti per generare un output di grandezza variabile. Il file Reviews.csv dovrebbe trovarsi nella stessa directory.

Eseguire il comando seguente se si desidera rimuovere la prima riga del file CSV
```
tail -n +2 data/Reviews.csv > data/Reviews1.csv
rm data/Reviews.csv
mv data/Reviews1.csv data/Reviews.csv
