# -*- coding: utf-8 -*-
"""
Typy raportów i struktury danych dla systemu szablonów.
Obsługuje dynamiczną liczbę tabel (3-7) w zależności od typu raportu.
"""

from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional


class ReportType(Enum):
    """Enum z 7 typami raportów"""
    ROZLICZENIE_A_KWARTAL = "rozliczenie_grupa_a_kwartal"
    ROZLICZENIE_B_KWARTAL = "rozliczenie_grupa_b_kwartal"
    ROZLICZENIE_B_MIESIAC = "rozliczenie_grupa_b_miesiac"
    WYKONANIE_A_TYP_A_TYDZIEN = "wykonanie_grupa_a_typ_a_tydzien"
    WYKONANIE_A_TYP_B_TYDZIEN = "wykonanie_grupa_a_typ_b_tydzien"
    WYKONANIE_B_TYP_A_TYDZIEN = "wykonanie_grupa_b_typ_a_tydzien"
    WYKONANIE_B_TYP_B_TYDZIEN = "wykonanie_grupa_b_typ_b_tydzien"
    # NOWY_TYP_RAPORTU = "nowy_typ_raportu"  # ← Dodaj tutaj


@dataclass
class TableConfig:
    """Konfiguracja pojedynczej tabeli danych"""
    name: str
    title: str
    headers: List[str]
    data: List[List[str]] = field(default_factory=list)
    currency_columns: List[int] = field(default_factory=list)
    percentage_columns: List[int] = field(default_factory=list)
    add_total_row: bool = True
    column_widths: Optional[List[float]] = None


@dataclass
class ReportDataV2:
    """
    Struktura danych dla raportu PDF - elastyczna liczba tabel.
    Obsługuje od 3 do 7 tabel danych + podsumowanie.
    """
    
    # Dane klienta
    client_name: str = ""
    
    # Podsumowanie (zawsze obecne)
    summary_title: str = "Podsumowanie"
    summary_labels: List[str] = field(default_factory=list)
    summary_data: List[List[str]] = field(default_factory=list)
    
    # Dynamiczne tabele danych
    data_tables: List[TableConfig] = field(default_factory=list)
    
    def add_table(self, table: TableConfig):
        """Dodaje tabelę do raportu"""
        self.data_tables.append(table)
    
    def get_table(self, name: str) -> Optional[TableConfig]:
        """Pobiera tabelę po nazwie"""
        for table in self.data_tables:
            if table.name == name:
                return table
        return None
    
    def table_count(self) -> int:
        """Zwraca liczbę tabel danych (bez podsumowania)"""
        return len(self.data_tables)
