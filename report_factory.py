# -*- coding: utf-8 -*-
"""
ReportFactory - ładuje konfigurację z YAML i generuje raporty PDF.
Obsługuje CALL procedura(param) jako źródło danych.
Jeden numer = jeden PDF. Zmiana numeru = nowy plik PDF.
"""

import yaml
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime
import os

from main import (
    get_hana_connection,
    get_output_folder,
    save_generation_log,
    generate_pdf,
    Timer,
    FONT_REGULAR,
    FONT_BOLD,
    TEXT_COLOR,
    BASE_OUTPUT_DIR,
)

from report_types import ReportType, TableConfig, ReportDataV2


class ReportFactory:
    """
    Factory do generowania raportów z dynamiczną liczbą tabel.
    Konfiguracja z plików YAML, dane z SAP HANA (CALL procedura).
    """

    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.configs: Dict[ReportType, dict] = {}
        self._load_all_configs()

    def _load_all_configs(self):
        """Ładuje wszystkie konfiguracje YAML."""
        print(f"\n📂 Konfiguracje: {self.config_dir.resolve()}")
        for report_type in ReportType:
            config_path = self.config_dir / f"{report_type.value}.yaml"
            if not config_path.exists():
                print(f"  ⚠️  Brak: {config_path.name}")
                continue
            with open(config_path, 'r', encoding='utf-8') as f:
                self.configs[report_type] = yaml.safe_load(f)
            tables = len(self.configs[report_type].get('data_tables', []))
            print(f"  ✓ {report_type.value} ({tables} tabel)")

    # ------------------------------------------------------------------
    #  POBIERANIE DANYCH Z HANA
    # ------------------------------------------------------------------

    def _execute_query(self, query: str, nr: str, lokalizacja: str = None) -> List[List[str]]:
        """
        Wykonuje CALL schema.procedura(nr) i zwraca result set.
        Parametr {nr} i {lok} w zapytaniu zostanie podstawiony wartością.
        """
        connection = get_hana_connection()
        cursor = connection.cursor()
        try:
            resolved_query = query.replace('{nr}', str(nr)).strip()
            if lokalizacja:
                resolved_query = resolved_query.replace('{lok}', str(lokalizacja))
            print(f"    SQL: {resolved_query}")
            cursor.execute(resolved_query)

            try:
                rows = cursor.fetchall()
                return [
                    [str(cell) if cell is not None else '' for cell in row]
                    for row in rows
                ]
            except Exception:
                return []
        finally:
            cursor.close()
            connection.close()

    def _fetch_summary(self, config: dict, nr: str) -> List[List[str]]:
        """Pobiera dane podsumowania (pary etykieta/wartość)."""
        query = config['summary']['query']
        labels = config['summary']['labels']
        rows = self._execute_query(query, nr)

        if not rows:
            return [[label, ''] for label in labels]

        # Pierwszy wiersz wyników → mapuj na etykiety
        values = rows[0]
        return [
            [label, values[i] if i < len(values) else '']
            for i, label in enumerate(labels)
        ]

    def _fetch_table_data(self, table_cfg: dict, nr: str,
                          lokalizacja: str = None) -> List[List[str]]:
        """Pobiera dane dla pojedynczej tabeli."""
        return self._execute_query(table_cfg['query'], nr, lokalizacja)

    # ------------------------------------------------------------------
    #  POBIERANIE LISTY UMÓW / KLIENTÓW
    # ------------------------------------------------------------------

    def get_contracts_list(self) -> List[Dict[str, Any]]:
        """
        Pobiera listę unikalnych umów z HANA.
        SELECT DISTINCT nr_umowy, klient, podtyp_klient FROM xx.xxd
        Zwraca listę słowników: [{'nr_umowy': ..., 'klient': ..., 'podtyp_klient': ...}, ...]
        """
        connection = get_hana_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(
                "SELECT DISTINCT nr_umowy, klient, podtyp_klient FROM xx.xxd"
            )
            rows = cursor.fetchall()
            contracts = []
            for row in rows:
                if row and len(row) >= 3:
                    contracts.append({
                        'nr_umowy': str(row[0]),
                        'klient': str(row[1]),
                        'podtyp_klient': str(row[2]),
                    })
            return contracts
        finally:
            cursor.close()
            connection.close()

    def get_clients_list(self) -> List[str]:
        """
        Zwraca listę unikalnych nr_umowy (kompatybilność wsteczna).
        """
        contracts = self.get_contracts_list()
        return [c['nr_umowy'] for c in contracts]

    def get_locations_for_contract(self, nr_umowy: str) -> List[str]:
        """
        Pobiera listę lokalizacji (sklepów) dla danej umowy.
        Używane przy Typ B – raport per lokalizacja.
        """
        connection = get_hana_connection()
        cursor = connection.cursor()
        try:
            cursor.execute(
                f"CALL SCHEMA.PROC_SKLEPY_WYKONANIE_A_X({nr_umowy})"
            )
            rows = cursor.fetchall()
            return list(set(str(row[0]) for row in rows if row))
        finally:
            cursor.close()
            connection.close()

    # ------------------------------------------------------------------
    #  GENEROWANIE RAPORTÓW
    # ------------------------------------------------------------------

    def generate_report(self, report_type: ReportType, nr: str,
                        lokalizacja: str = None) -> str:
        """
        Generuje pojedynczy raport PDF dla danego numeru umowy.

        Args:
            report_type: Typ raportu (enum)
            nr: Numer umowy (parametr do CALL procedura)
            lokalizacja: Nazwa lokalizacji (tylko dla Typ B)

        Returns:
            Ścieżka do wygenerowanego PDF
        """
        if report_type not in self.configs:
            raise ValueError(f"Brak konfiguracji: {report_type.value}")

        config = self.configs[report_type]
        print(f"\n→ {config['report_type']} | NR={nr}")

        # Pobierz podsumowanie
        with Timer("  Podsumowanie"):
            summary_data = self._fetch_summary(config, nr)

        # Nazwa klienta z pierwszego wiersza podsumowania (jeśli jest)
        client_name = summary_data[0][1] if summary_data and len(summary_data[0]) > 1 else str(nr)

        # Pobierz dane tabel
        tables = []
        for table_cfg in config['data_tables']:
            with Timer(f"  {table_cfg['title']}"):
                data = self._fetch_table_data(table_cfg, nr, lokalizacja)
                fmt = table_cfg.get('formatting', {})
                tables.append({
                    'title': table_cfg['title'],
                    'subtitle': table_cfg.get('subtitle', ''),
                    'headers': table_cfg['headers'],
                    'data': data,
                    'currency_columns': fmt.get('currency_columns', []),
                    'percentage_columns': fmt.get('percentage_columns', []),
                    'add_total_row': fmt.get('add_total_row', True),
                    'column_widths': fmt.get('column_widths'),
                })

        # Generuj PDF — każdy NR = osobny plik
        safe_nr = str(nr).replace('/', '_').replace('\\', '_')
        if lokalizacja:
            safe_lok = lokalizacja.replace(' ', '_').replace('/', '_')
            filename = f"{report_type.value}_{safe_nr}_{safe_lok}.pdf"
        else:
            filename = f"{report_type.value}_{safe_nr}.pdf"
        subfolder = report_type.value.replace('_', '#')

        with Timer(f"  PDF ({len(tables)} tabel)"):
            pdf_path = generate_pdf(
                client_name=client_name,
                summary_title=config['summary']['title'],
                summary_data=summary_data,
                tables=tables,
                filename=filename,
                subfolder=subfolder,
            )

        return pdf_path

    def generate_all_for_nr(self, nr: str,
                            report_types: List[ReportType] = None) -> List[str]:
        """
        Generuje wybrane (lub wszystkie) typy raportów dla numeru umowy.

        Typ A = generuje dla każdej umowy (wszystkie sklepy)
        Typ B = generuje dla każdej lokalizacji osobno

        Args:
            nr: Numer umowy
            report_types: Lista typów (None = wszystkie skonfigurowane)

        Returns:
            Lista ścieżek do wygenerowanych PDF
        """
        types = report_types or list(self.configs.keys())
        generated = []

        for rt in types:
            if rt not in self.configs:
                print(f"  ⚠️  Pomijam {rt.value} - brak konfiguracji")
                continue
            try:
                # Typ B – generuj per lokalizacja
                if rt in (ReportType.WYKONANIE_A_TYP_B_TYDZIEN,
                          ReportType.WYKONANIE_B_TYP_B_TYDZIEN):
                    locations = self.get_locations_for_contract(nr)
                    print(f"  📍 Lokalizacje ({len(locations)}): {locations}")
                    for lok in locations:
                        try:
                            path = self.generate_report(rt, nr, lokalizacja=lok)
                            generated.append(path)
                        except Exception as e:
                            print(f"  ✗ {rt.value} lok={lok}: {e}")
                else:
                    # Typ A – jeden raport per umowa
                    path = self.generate_report(rt, nr)
                    generated.append(path)
            except Exception as e:
                print(f"  ✗ {rt.value}: {e}")

        return generated

