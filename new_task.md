1. Mock bazy danych sap hana - dokladnie tak jakby to dzialalo , zebym tylko pozniej podmienil dane string connect i ma dzialac. - nie mam jak w tej chwili sie podlaczyc.

2. Tabela do pobrania listy Nr umow, klioentow , typow 

Select distinct
Nr_umowy
klient
podtyp_klient
from xx.xxd

3. na ten moment skupimy sie tylko na 2 szablonach z wykonaniem. Każda tabela bedzia miala swojgo selecta z result setem, ale tam są wszyscy klienci!

 - "Wykonanie Grupa A - Typ A - Tydzień" - generujemy dla każdej umowy !

Ma dzialc nastepująco:

Napis Twoj raport oraz data, obok tabela z danymi do raportu (podsumowanie)

(8x2)
Nr umowy    string 
Typ         string
Nazwa       string
Lok         string
Rok         string
Kwar        string
DR          string
EMail       string

Tabela 2 - Naglowek - duza
(5x2)
Rabat       int - formatowenie Procent
Rabat       int - formatowenie Procent
Udzielony   int -  formatowenie zł - zaok do 2 miejsc po przecinku
Wartosc     int -  formatowenie zł - zaok do 2 miejsc po przecinku
Wyplata int -  formatowenie zł - zaok do 2 miejsc po przecinku

Tabela 3 - Naglowek
Podnaglowek - grupa umowa -> x
10xn - n liczba sklepow 
select distinct
Nazwa sklepu
grupa umowa
Klasyfikacja
typ lok 
podstawa int zł
bonus int %
wyprac int %
bonus lacz int %
wartosc bonsu int zł
udzieliony rabat int zł
wartosc do wyr int zł 
SUMA - dodac 
 Tabela 3 - Naglowek
Podnaglowek - grupa umowa -> y
10xn - n liczba sklepow 
select distinct
Nazwa sklepu
grupa umowa
Klasyfikacja
typ lok 
podstawa int zł
bonus int %
wyprac int %
bonus lacz int %
wartosc bonsu int zł
udzieliony rabat int zł
wartosc do wyr int zł 
SUMA - dodac 
 Tabela 3 - Naglowek
Podnaglowek - grupa umowa -> z
10xn - n liczba sklepow 
select distinct
Nazwa sklepu
grupa umowa
Klasyfikacja
typ lok 
podstawa int zł
bonus int %
wyprac int %
bonus lacz int %
wartosc bonsu int zł
udzieliony rabat int zł
wartosc do wyr int zł 
SUMA - dodac 
                







- "Wykonanie Grupa A - Typ B - Tydzień" - generujemy dla lokalizacji !

zamiast wszystkich sklepow pokazujemy tylko wybrany - reszta to samo 